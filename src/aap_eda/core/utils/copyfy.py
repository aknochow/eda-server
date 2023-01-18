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
"""Utilities for writing model data to a file.

These functions will help with writing model instance data to a
file for use with the `copy_to_table` function which uses a
fast bulk data loading database command.
"""

import json
from typing import Any, Iterable, List, Optional, TextIO, Tuple, Union

from django.contrib.postgres.fields import ArrayField
from django.db import ConnectionProxy, models
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import JSONField

DEFAULT_SEP = "\t"
DEFAULT_NULL = "\\N"


class Copyfy:
    ALLOWED_TYPES = ()

    def __init__(self, wrapped: Any):
        if (
            wrapped is not None
            and len(self.ALLOWED_TYPES) > 0
            and not isinstance(wrapped, self.ALLOWED_TYPES)
        ):
            allowed_types = ", ".join(self.ALLOWED_TYPES)
            raise TypeError(
                f"wrapped must be one of these instances: {allowed_types}"
            )

        self.wrapped = wrapped

    def __str__(self):
        if self.wrapped is None:
            return DEFAULT_NULL

        return self.stringify(self.wrapped)

    def stringify(self, wrapped: Any) -> str:
        return str(wrapped)


class CopyfyDict(Copyfy):
    ALLOWED_TYPES = (dict,)

    def stringify(self, wrapped: Any) -> str:
        out = json.dumps(self.wrapped)
        if len(out) == 0:
            out = "{}"  # noqa: P103

        return out


class CopyfyListTuple(Copyfy):
    ALLOWED_TYPES = (list, tuple)

    def stringify(self, wrapped: Union[List, Tuple]) -> str:
        out = []

        for val in wrapped:
            if isinstance(val, (list, tuple)):
                str_val = self.stringify(val)
            elif isinstance(val, dict):
                str_val = str(CopyfyDict(val))
            else:
                str_val = super().stringify(val)

            out.append(str_val)

        return f"{{{','.join(out)}}}"


ADAPT_COPY_MAP = {
    dict: CopyfyDict,
    list: CopyfyListTuple,
    tuple: CopyfyListTuple,
}


def adapt_copy_types(values: Iterable[Any]) -> List[Any]:
    return [ADAPT_COPY_MAP.get(type(val), Copyfy)(val) for val in values]


def copyfy_values(
    values: Union[List, Tuple],
    *,
    sep: str = DEFAULT_SEP,
) -> str:
    """Return a PostgreSQL string representation of the specified objects."""
    if not values:
        return ""

    return sep.join(str(v) for v in adapt_copy_types(values))


def copy_to_table(
    conn: Union[BaseDatabaseWrapper, ConnectionProxy],
    db_table: str,
    columns: Union[List[str], Tuple[str]],
    data: TextIO,
    *,
    sep: str = DEFAULT_SEP,
) -> None:
    """Read data from a file-like object and write to DB table using COPY."""
    with conn.cursor() as cur:
        cur.cursor.copy_from(
            data,
            db_table,
            sep=sep,
            null=DEFAULT_NULL,
            columns=columns,
        )


def get_column_names(model: models.Model) -> List[str]:
    """Return a list of concretely defined columns."""
    return [f.name for f in model._meta.concrete_fields]


def get_column_values(
    model: models.Model, columns: List[str], wrap: bool = True
) -> List[str]:
    """Return a list of the values for the specified columns."""
    res = []

    for col in columns:
        value = getattr(model, col, None)
        if wrap:
            field = model._meta.get_field(col)
            if isinstance(field, JSONField):
                value = CopyfyDict(value)
            elif isinstance(field, ArrayField):
                value = CopyfyListTuple(value)

        res.append(value)

    return res


def copyfy(
    model: models.Model,
    *,
    sep: str = DEFAULT_SEP,
    fields: Optional[Iterable[str]] = None,
):
    """Convert model instance into PostgreSQL COPY format."""
    if fields is None:
        fields = get_column_names(model)
    vals = get_column_values(model, fields)
    return copyfy_values(vals, sep=sep)


__all__ = ("copyfy",)
