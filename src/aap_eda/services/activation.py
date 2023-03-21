#  Copyright 2023 Red Hat, Inc.
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

import logging
import os
import shutil
import subprocess
from typing import Dict

import docker

from aap_eda.api.serializers import ActivationInstanceSerializer
from aap_eda.core.enums import ActivationStatus
from aap_eda.core.models import ActivationInstance, Ruleset
from aap_eda.settings.default import (
    AnsibleRunnerDeployment,
    AnsibleRunnerSettings,
)
from aap_eda.tasks import ActivationExecution, consume_log

LOG = logging.getLogger(__name__)

activated_rulesets = {}


def ensure_directory(directory):
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def activate_rulesets(activation_instance: ActivationInstance):
    deployment_type = AnsibleRunnerSettings.deployment_type
    ruleset = Ruleset.objects.filter(
        rulebook_id=activation_instance.activation.rulebook_id
    ).first()
    rs_source = ruleset.sources[0] if ruleset.sources else {}

    LOG.debug(f"Ansible Runner deployment type = {deployment_type}")
    if deployment_type == AnsibleRunnerDeployment.LOCAL:
        execution = local_activate_rulesets(
            activation_instance, ruleset, rs_source
        )
    elif deployment_type == AnsibleRunnerDeployment.DOCKER:
        execution = docker_activate_rulesets(
            activation_instance, ruleset, rs_source
        )
    else:
        raise NotImplementedError(
            f"Deployment type {deployment_type} has not been implemented"
        )

    job = consume_log.delay(
        log_source=execution,
        activation_instance=activation_instance,
    )

    return job


def local_activate_rulesets(
    activation_instance: ActivationInstance,
    ruleset: Ruleset,
    ruleset_source: Dict,
) -> ActivationExecution:
    ansible_rulebook = shutil.which("ansible-rulebook")
    if ansible_rulebook is None:
        raise Exception("ansible-rulebook not found")

    local_working_directory = activation_instance.activation.working_directory
    ensure_directory(local_working_directory)
    host = ruleset_source.get("config", {}).get(
        "host", AnsibleRunnerSettings.deployment_host
    )
    port = ruleset_source.get("config", {}).get(
        "port", AnsibleRunnerSettings.deployment_port
    )

    cmd_args = [
        ansible_rulebook,
        "--worker",
        "--websocket-address",
        f"ws://{host}:{port}/api/ws2",
        "--id",
        str(activation_instance.id),
    ]

    LOG.debug(ansible_rulebook)
    LOG.debug(cmd_args)
    LOG.info("Launching ansible-rulebook subprocess")

    proc = subprocess.Popen(
        cmd_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    activated_rulesets[activation_instance.id] = proc

    return ActivationExecution(AnsibleRunnerDeployment.LOCAL, subprocess=proc)


def docker_activate_rulesets(
    activation_instance: ActivationInstance,
    ruleset: Ruleset,
    ruleset_source: Dict,
) -> ActivationExecution:
    activation = activation_instance.activation
    host = ruleset_source.get("config", {}).get(
        "host", AnsibleRunnerSettings.deployment_host
    )
    port = ruleset_source.get("config", {}).get(
        "port", AnsibleRunnerSettings.deployment_port
    )

    LOG.info("Launching ansible-rulebook container")
    LOG.debug("Host: %s", host)
    LOG.debug("Port: %s", port)

    command = [
        "ssh-agent",
        "ansible-rulebook",
        "--worker",
        "--websocket-address",
        f"ws://{host}:{port}/api/ws2",
        "--id",
        str(activation.id),
        "--debug",
    ]

    docker_client = docker.DockerClient(
        base_url="unix:///var/run/docker.sock", version="auto", timeout=2
    )
    img = docker_client.images.pull(activation.execution_environment)
    container = docker_client.containers.create(
        img,
        command,
        environment=["ANSIBLE_FORCE_COLOR=True"],
        extra_hosts={"host.docker.internal": "host-gateway"},
        ports={f"{port}/tcp": port, "8000/tcp": None},
        network="eda-network",
        detach=True,
        remove=True,
    )
    container.start()

    return ActivationExecution(
        AnsibleRunnerDeployment.DOCKER,
        docker_client=docker_client,
        container=container,
    )


def create_activation_instance_record(
    new_activation_instance: ActivationInstanceSerializer,
) -> ActivationInstance:
    """Create the activation record from the serializer data."""
    act_inst = new_activation_instance.create()
    return act_inst


def create_activation_instance(
    new_activation_instance: ActivationInstanceSerializer,
):
    """Create activation_instance record and activate rulesets."""
    act_inst = create_activation_instance_record(new_activation_instance)
    job = activate_rulesets(act_inst)
    if job:
        act_inst.status = ActivationStatus.RUNNING
        act_inst.save()

    return job


def deactivate_activation_instance(activation_instance: ActivationInstance):
    key = activation_instance.id
    try:
        execution = activated_rulesets.pop(key)
    except KeyError:
        raise ProcessLookupError(
            f"Activation instance ({key}) has no running processes."
        )
    else:
        if execution.consumer is not None:
            execution.consumer.interrupt()

        if execution.deployment_type == AnsibleRunnerDeployment.LOCAL:
            execution.subprocess.kill()
        elif execution.deployment_type == AnsibleRunnerDeployment.DOCKER:
            execution.container.kill()
            execution.docker_client.close()
