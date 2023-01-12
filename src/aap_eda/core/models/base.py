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

import json
from typing import Any, List, Optional, TextIO, Tuple, Union

from django.contrib.postgres.fields import ArrayField
from django.db import ConnectionProxy, models
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import JSONField


DEFAULT_SEP = "\t"
DEFAULT_NULL = "\\N"


class Copyfy:
    TYPE_LOCK = None

    def __init__(self, wrapped: Any, null: str = DEFAULT_NULL):
        if self.TYPE_LOCK is not None and not isinstance(wrapped, self.TYPE_LOCK):
            raise TypeError(
                f"wrapped must be one of these instances: {', '.join(self.TYPE_LOCK)}"
            )
        self.wrapped = wrapped

    def __str__(self):
        return self.stringify(self.wrapped)

    def stringify(self, wrapped: Any):
        if wrapped is None:
            return DEFAULT_NULL

        return str(wrapped)


class CopyfyDict(Copyfy):
    TYPE_LOCK = (dict, type(None))

    def stringify(self, wrapped: Any):
        if self.wrapped is None:
            return DEFAULT_NULL

        out = json.dumps(self.wrapped)
        if len(out) == 0:
            out = "{}"

        return out


class CopyfyListTuple(Copyfy):
    TYPE_LOCK = (list, tuple, type(None))

    def stringify(self, wrapped: Union[List, Tuple]) -> str:
        if self.wrapped is None:
            return DEFAULT_NULL

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


def adapt_copy_types(values: Union[List, Tuple]) -> Union[List, Tuple]:
    out = [
        CopyfyDict(val)
        if isinstance(val, dict)
        else CopyfyListTuple(val)
        if isinstance(val, (list, tuple))
        else Copyfy(val)
        if val is None
        else val
        for val in values
    ]

    if isinstance(values, tuple):
        return tuple(out)

    return out


def copyfy_values(
    values: Union[List, Tuple],
    *,
    sep: str = DEFAULT_SEP,
) -> str:
    """Return a PostgreSQL string representation of the specified objects."""
    if not values:
        return ""

    return DEFAULT_SEP.join(str(v) for v in adapt_copy_types(values))


def copy_to_table(
    conn: Union[BaseDatabaseWrapper, ConnectionProxy],
    db_table: str,
    columns: Union[List[str], Tuple[str]],
    data: TextIO,
    *,
    sep: str = DEFAULT_SEP,
) -> bool:
    """Read data from a file-like object and write to DB table using COPY."""
    with conn.cursor() as cur:
        res = cur.cursor.copy_from(
            data,
            db_table,
            sep=sep,
            null=DEFAULT_NULL,
            columns=columns,
        )

    return True


class CopyfyMixin:
    """Provide methods to help write model data to a file.

    These methods will help with writing model instance data to a
    file for use with the `copy_to_table` function which uses a
    fast bulk data loading database command.
    """

    def get_column_names(self) -> Tuple[str]:
        """Return a list of concretely defined columns."""
        return [f.name for f in self._meta.concrete_fields]

    def get_column_values(self, columns: List, wrap: bool = True) -> List[str]:
        """Return a list of the values for the specified columns."""
        res = []

        for col in columns:
            value = getattr(self, col, None)
            if wrap:
                field = self._meta.get_field(col)
                if isinstance(field, JSONField):
                    value = CopyfyDict(value)
                elif isinstance(field, ArrayField):
                    value = CopyfyListTuple(value)

            res.append(value)

        return res

    def mogrify(
        self,
        *,
        sep: str = DEFAULT_SEP,
        fields: Tuple[str] = (),
    ) -> str:
        if not fields:
            fields = self.get_column_names()

        vals = self.get_column_values(fields)
        return mogrify_values(vals, sep=sep)


OIDField = models.IntegerField


__all__ = (
    "Copyfy",
    "CopyfyDict",
    "CopyfyListTuple",
    "CopyfyMixin",
    "OIDField",
    "copy_to_table",
    "adapt_copy_types",
    "copyfy_values",
)
