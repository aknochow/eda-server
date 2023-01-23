#  Copyright 2022 Red Hat, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Project services.
"""
# TODO(cutwater): This module is mostly a port from eda-server repository.
#   It requires patches for testing due to lack of dependency injection.
#   This module requires full refactoring.
import enum
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Iterator, List, Optional, Union

import yaml
from django.db import transaction

from aap_eda.core import models

__all__ = (
    "import_project",
    "sync_project",
)


logger = logging.getLogger(__name__)

StrPath = Union[str, os.PathLike[str]]

TMP_PREFIX: Final = "eda-project"


@transaction.atomic
def import_project(
    url: str,
    name: str,
    description: str,
) -> None:
    with tempfile.TemporaryDirectory(preifx=TMP_PREFIX) as repo_dir:
        commit_id = clone_project(url, repo_dir)
        project = models.Project.objects.create(
            url=url,
            commit_id=commit_id,
            name=name,
            description=description,
        )
        import_files(project, repo_dir)
        save_project_archive(project, repo_dir)


def sync_project() -> None:
    raise NotImplementedError


# Utility functions
# ---------------------------------------------------------
IGNORED_DIRS = (".git", ".github")
YAML_EXTENSIONS = (".yml", ".yaml")


def clone_project(url: str, path: StrPath) -> str:
    """Clone repository and return current commit id."""
    git_shallow_clone(url, path)
    return git_current_commit(path)


class EntryKind(enum.Enum):
    RULEBOOK = "rulebook"
    INVENTORY = "inventory"
    EXTRA_VARS = "extra_vars"
    PLAYBOOK = "playbook"


@dataclass(slots=True)
class ScanResult:
    kind: EntryKind
    filename: str
    raw_content: str
    content: Any


def scan_project(path: StrPath) -> Iterator[ScanResult]:
    for root, dirs, files in os.walk(path):
        # Skip ignored directories
        dirs[:] = [x for x in dirs if x not in IGNORED_DIRS]
        root = Path(root)
        for filename in files:
            path = root.joinpath(filename)
            try:
                result = scan_file(path)
            except Exception:
                logger.exception(
                    "Unexpected exception when scanning file %s", path
                )
                continue
            if result is not None:
                yield result


def save_project_archive(project: models.Project, path: StrPath):
    raise NotImplementedError


def scan_file(path: Path) -> Optional[ScanResult]:
    if path.suffix not in YAML_EXTENSIONS:
        return None

    try:
        with path.open() as f:
            raw_content = f.read()
    except OSError as exc:
        logger.warning("Cannot open file %s: %s", path.name, exc)
        return None

    try:
        content = yaml.safe_load(raw_content)
    except yaml.YAMLError as exc:
        logger.warning("Invalid YAML file %s: %s", path.name, exc)
        return None

    kind = guess_entry_kind(content)
    if kind is None:
        return None
    return ScanResult(
        kind=kind,
        filename=path.name,
        raw_content=raw_content,
        content=content,
    )


def guess_entry_kind(data: Any) -> EntryKind:
    if is_rulebook_file(data):
        return EntryKind.RULEBOOK
    elif is_inventory_file(data):
        return EntryKind.INVENTORY
    elif is_playbook_file(data):
        return EntryKind.PLAYBOOK
    else:
        return EntryKind.EXTRA_VARS


def is_rulebook_file(data: Any) -> bool:
    if not isinstance(data, list):
        return False
    return all("rules" in entry for entry in data)


def is_playbook_file(data: Any) -> bool:
    if not isinstance(data, list):
        return False
    for entry in data:
        if not isinstance(entry, dict):
            return False
        if entry.keys() & {"tasks", "roles"}:
            return True
    return False


def is_inventory_file(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    return "all" in data


def import_files(project: models.Project, path: StrPath):
    for entry in scan_project(path):
        handler = IMPORT_HANDLERS[entry.kind]
        handler(project, entry)


def import_inventory(
    project: models.Project, entry: ScanResult
) -> models.Inventory:
    raise NotImplementedError


def import_extra_var(
    project: models.Project, entry: ScanResult
) -> models.ExtraVar:
    raise NotImplementedError


def import_playbook(
    project: models.Project, entry: ScanResult
) -> models.Playbook:
    raise NotImplementedError


def import_rulebook(
    project: models.Project, entry: ScanResult
) -> models.Rulebook:
    raise NotImplementedError


IMPORT_HANDLERS = {
    EntryKind.INVENTORY: import_inventory,
    EntryKind.EXTRA_VARS: import_extra_var,
    EntryKind.PLAYBOOK: import_playbook,
    EntryKind.RULEBOOK: import_rulebook,
}

# Query functions
# ---------------------------------------------------------


# Git functions
# ---------------------------------------------------------
GIT_BIN: Final = shutil.which("git")

GIT_CLONE_TIMEOUT: Final = 30
GIT_TIMEOUT: Final = 30
GIT_ENVIRON: Final = {
    "GIT_TERMINAL_PROMPT": "0",
}


class GitCommandFailed(Exception):
    pass


def git_command(
    args: List[str],
    *,
    capture_output: bool = False,
    timeout: Optional[float] = None,
    cwd: Optional[StrPath] = None,
) -> subprocess.CompletedProcess:
    """Git command wrapper."""
    try:
        result = subprocess.run(
            args,
            check=True,
            encoding="utf-8",
            env=GIT_ENVIRON,
            stderr=subprocess.PIPE,
            capture_output=capture_output,
            timeout=timeout,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired as exc:
        logging.warning("%s", str(exc))
        raise GitCommandFailed("timeout")
    except subprocess.CalledProcessError as exc:
        message = (
            f"Command returned non-zero exit status {exc.returncode}:"
            f"\n\tcommand: {exc.cmd}"
            f"\n\tstderr: {exc.stderr}"
        )
        logger.warning("%s", message)
        raise GitCommandFailed(exc.stderr)
    return result


def git_shallow_clone(url: str, path: StrPath) -> None:
    """Create a shallow clone of the repository in the specified directory.

    :param url: The repository URL to clone from.
    :param path: The directory to clone into.
    :raises GitCommandFailed: If git returns non-zero exit code.
    """
    cmd = [GIT_BIN, "clone", "--quiet", "--depth", "1", url, os.fspath(path)]
    git_command(cmd)


def git_current_commit(path: os.PathLike) -> str:
    """Return the object name of the current commit

    :param path: Path to the repository.
    :return: Current commit id.
    :raises GitCommandFailed: If git returns non-zero exit code.
    """
    cmd = [GIT_BIN, "rev-parse", "HEAD"]
    result = git_command(cmd, capture_output=True, cwd=path)
    return result.stdout.strip()
