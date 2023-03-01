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

import os
from datetime import datetime, timezone
from enum import Enum
from typing import List

from django.db import IntegrityError, connection, models

LETTERS = "abcdefghijklmnopqrstuvwxyz"
ALIASES = [f"{'_' * i}{letter}" for i in range(1, 3) for letter in LETTERS]


def apply_model_defaults(model: models.Model, record: dict) -> dict:
    now = datetime.now(tz=timezone.utc)
    full_record = record.copy()

    for field in model._meta.concrete_fields:
        if field.primary_key:
            continue

        if isinstance(field, models.ForeignKey):
            fname = field.get_attname_column()[-1]
        else:
            fname = field.name

        if getattr(field, "auto_now_add", False) or getattr(
            field, "auto_now", False
        ):
            default = now
        else:
            default = field.default

        if default != models.NOT_PROVIDED:
            if fname not in record:
                if callable(default):
                    full_record[fname] = default()
                elif isinstance(default, Enum):
                    full_record[fname] = str(default.value)
                else:
                    full_record[fname] = default

        if not field.null and full_record.get(fname) is None:
            raise ValueError(f"{model.__name__}.{fname} cannot be None.")

    return full_record


def fk_or_model_pk(fkeys: List[models.Field], record: dict) -> dict:
    for f in fkeys:
        if f.name in record:
            if isinstance(record[f.name], models.Model):
                record[f.name] = getattr(
                    record[f.name], f.target_field.name, None
                )
    return record


def build_insert(
    model: models.Model, fkeys: List[models.Field], record: dict
) -> str:
    """Build the base insert."""
    field_to_target_col = {f.name: f.get_attname_column()[-1] for f in fkeys}
    return """
insert into {table} ({column_list})
values ({data_list})
returning *
    """.format(
        table=model._meta.db_table,
        column_list=", ".join(
            field_to_target_col.get(col, col) for col in record
        ),
        data_list=", ".join(f"%({col})s" for col in record),
    )


def build_insert_validated_select(
    model: models.Model, record: dict, fetch_related: bool = False
) -> str:
    """Wrap the base insert in a CTE for related table data verification.

    Immediate relations of the model can be fetched as json. (fetch_related)
    """
    remote_fields = []
    fk_fields = [
        {
            "remote_alias": ALIASES[fnum],
            "join_local_key": f"_nr.{f.get_attname_column()[-1]}",
            "local_key": f.get_attname_column()[-1],
            "remote_table": f.related_model._meta.db_table,
            "remote_key": f"{ALIASES[fnum]}.{f.target_field.name}",
            "remote_row": f"row_to_json({ALIASES[fnum]}) as {f.related_model._meta.db_table}_rec",  # noqa E501
            "_field": f,
        }
        for fnum, f in enumerate(model._meta.concrete_fields)
        if isinstance(f, models.ForeignKey)
    ]
    local_fields = [
        f"_nr.{f.name}"
        for f in model._meta.concrete_fields
        if not isinstance(f, models.ForeignKey)
    ]
    remote_fields = [
        f"{fk['remote_key']} as {fk['local_key']}" for fk in fk_fields
    ]
    if fetch_related:
        remote_fields.extend(fk["remote_row"] for fk in fk_fields)
    if fk_fields:
        left_joins = f"{os.linesep}  ".join(
            "left join {remote_table} as {remote_alias} "
            "on {remote_key} = {join_local_key}".format(**fk)
            for fk in fk_fields
        )

    sep_indent = f",{os.linesep}       "
    select_cols = sep_indent.join(local_fields + remote_fields)
    insert_sql = build_insert(model, [f["_field"] for f in fk_fields], record)
    return f"""
with new_rec as (
{insert_sql}
)
select {select_cols}
  from new_rec as _nr
  {left_joins}
;
    """


def validated_create(
    model: models.Model, record: dict, fetch_related: bool = False
) -> models.Model:
    """Create record for model and verify existence of related data."""
    record = apply_model_defaults(model, record)
    fk_fields = [
        f
        for f in model._meta.concrete_fields
        if isinstance(f, models.ForeignKey)
    ]
    record = fk_or_model_pk(fk_fields, record)
    val_ins_sel_sql = build_insert_validated_select(
        model, record, fetch_related=fetch_related
    )
    val_rec = None

    with connection.cursor() as cur:
        cur.execute(val_ins_sel_sql, record)
        val_rec = dict(zip([d[0] for d in cur.description], cur.fetchone()))

    # Validate foreign key existence for any foreign keys in the input.
    # This is necessary in case the SQL is executed as part of a transaction
    # which will result in deferred constraint validation as django sets
    # foreign key constraints as deferred.
    for f in fk_fields:
        if f.name in record:
            fkname = f.name
        elif f.get_attname_column()[-1] in record:
            fkname = f.get_attname_column()[-1]
        else:
            continue
        vfkname = f.get_attname_column()[-1]

        if val_rec[vfkname] != record[fkname]:
            related_table = f"{f.related_model._meta.db_table}"
            msg = "is not present in table"
            raise IntegrityError(
                f"Key ({fkname})=({record[fkname]}) {msg} {related_table}"
            )

        # instantiate related model reference if it exists and is not None
        remote_table_name = f.related_model._meta.db_table
        remote_table_ref = f"{remote_table_name}_rec"
        if remote_table_ref in val_rec:
            remote_table_rec = val_rec.pop(remote_table_ref)
            if not remote_table_rec:
                val_rec[f.name] = f.related_model(**remote_table_rec)

    return model(**val_rec)
