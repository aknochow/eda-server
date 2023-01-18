import datetime
import json
from io import StringIO

import pytest
from django.contrib.postgres.fields import ArrayField
from django.db import connection, models
from psycopg2 import DatabaseError, DataError, connect as pg_connect

from aap_eda.core.models import Project
from aap_eda.core.utils import copyfy


class PgCursorWrapper:
    def __init__(self, cur):
        self.cursor = cur

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.cursor.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params)


class PgConnectionWrapper:
    def __init__(self, dsn):
        self.connection = pg_connect(dsn)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.connection.close()

    def cursor(self):
        return PgCursorWrapper(self.connection.cursor())

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()


def get_wrapped_connection():
    """Get a separate connection to tests exceptions."""
    settings = connection.settings_dict
    db_url = (
        "postgresql://{user}:{password}@{host}:{port}/{name}?{query}"
    ).format(
        user=settings["USER"],
        password=settings["PASSWORD"],
        host=settings["HOST"],
        port=settings["PORT"],
        name=settings["NAME"],
        query="sslmode=prefer&application_name=eda_test_exc",
    )
    return PgConnectionWrapper(db_url)


def test_copyfy_class():
    assert str(copyfy.Copyfy(None)) == copyfy.DEFAULT_NULL
    assert str(copyfy.Copyfy("eek")) == "eek"
    assert str(copyfy.Copyfy(1)) == "1"


def test_copyfydict():
    d = {"star": "trek", "quatloos": 200}
    assert str(copyfy.CopyfyDict(d)) == json.dumps(d)
    assert str(copyfy.CopyfyDict({})) == "{}"  # noqa: P103
    assert str(copyfy.CopyfyDict(None)) == copyfy.DEFAULT_NULL


def test_copyfylisttuple():
    assert str(copyfy.CopyfyListTuple(list("asdf"))) == "{a,s,d,f}"
    assert str(copyfy.CopyfyListTuple([])) == "{}"  # noqa: P103
    assert str(copyfy.CopyfyListTuple(None)) == copyfy.DEFAULT_NULL
    assert str(copyfy.CopyfyListTuple([[1, 2], [3, 4]])) == "{{1,2},{3,4}}"


def test_adapt_copy_types():
    vals = [
        1,
        "eek",
        None,
        {"feels": "meh"},
        datetime.datetime.now(tz=datetime.timezone.utc),
        [1, 2, 3, 4],
    ]
    mvals = copyfy.adapt_copy_types(vals)
    assert type(mvals) == list
    assert len(mvals) == len(vals)
    assert type(mvals[0]) == copyfy.Copyfy
    assert type(mvals[1]) == copyfy.Copyfy
    assert type(mvals[2]) == copyfy.Copyfy
    assert type(mvals[2]) == copyfy.Copyfy
    assert type(mvals[3]) == copyfy.CopyfyDict
    assert type(mvals[4]) == copyfy.Copyfy
    assert type(mvals[5]) == copyfy.CopyfyListTuple

    mvals = copyfy.adapt_copy_types(tuple(vals))
    assert type(mvals) == tuple


def test_copyfy_values(db):
    vals = [
        1,
        "eek",
        None,
        {"feels": "meh"},
        datetime.datetime.now(tz=datetime.timezone.utc),
        [1, 2, 3, 4],
    ]
    mvals = copyfy.copyfy_values(vals)
    assert isinstance(mvals, str)
    assert copyfy.DEFAULT_SEP in mvals
    mvals_list = mvals.split(copyfy.DEFAULT_SEP)
    assert len(mvals_list) == len(vals)
    assert mvals_list[0] == str(vals[0])
    assert mvals_list[1] == str(vals[1])
    assert mvals_list[2] == copyfy.DEFAULT_NULL
    assert mvals_list[3] == json.dumps(vals[3])
    assert mvals_list[4] == str(vals[4])
    assert mvals_list[5] == str(copyfy.CopyfyListTuple(vals[5]))


CREATE_TABLE_EEK = """
create table eek (
    id serial primary key,
    label text not null,
    data jsonb,
    lista int[],
    created_ts timestamptz
);
"""

DROP_TABLE_EEK = """
drop table eek;
"""


@pytest.fixture
def eek_table():
    with connection.cursor() as cur:
        cur.execute(CREATE_TABLE_EEK)
    yield
    with connection.cursor() as cur:
        cur.execute(DROP_TABLE_EEK)


@pytest.fixture
def eek_model(eek_table):
    class Eek(models.Model):
        class Meta:
            app_label = "core"
            db_table = "eek"

        label = models.TextField(null=False)
        data = models.JSONField()
        lista = ArrayField(models.IntegerField())
        created_ts = models.DateTimeField()

    return Eek


@pytest.mark.django_db
def test_copy_to_table(eek_model):
    cols = ["label", "data", "lista", "created_ts"]
    vals = [
        [
            "label-1",
            {"type": "rulebook", "data": {"ruleset": "ruleset-1"}},
            None,
            datetime.datetime.now(tz=datetime.timezone.utc),
        ],
        [
            "label-2",
            None,
            [1, 2, 3, 4],
            datetime.datetime.now(tz=datetime.timezone.utc),
        ],
    ]

    copy_file = StringIO()
    for val in vals:
        print(copyfy.copyfy_values(val), file=copy_file)  # noqa:T201
    copy_file.seek(0)

    copyfy.copy_to_table(connection, "eek", cols, copy_file)

    res = list(eek_model.objects.values_list())
    assert len(res) == 2
    for i, rec in enumerate(res):
        val = vals[i]
        for j in range(len(rec)):
            if j == 0:
                assert isinstance(rec[j], int)
            else:
                assert val[j - 1] == rec[j]


@pytest.mark.django_db
def test_copy_to_table_with_sep(eek_model):
    cols = ["label", "data", "lista", "created_ts"]
    vals = [
        [
            "label-1",
            {"type": "rulebook", "data": {"ruleset": "ruleset-1"}},
            None,
            datetime.datetime.now(tz=datetime.timezone.utc),
        ],
        [
            "label-2",
            None,
            [1, 2, 3, 4],
            datetime.datetime.now(tz=datetime.timezone.utc),
        ],
    ]

    copy_file = StringIO()
    for val in vals:
        copy_rec = copyfy.copyfy_values(val, sep="|")
        print(copy_rec, file=copy_file)  # noqa:T201
    copy_file.seek(0)

    copyfy.copy_to_table(connection, "eek", cols, copy_file, sep="|")

    res = list(eek_model.objects.values_list())
    assert len(res) == 2
    for i, rec in enumerate(res):
        val = vals[i]
        for j in range(len(rec)):
            if j == 0:
                assert isinstance(rec[j], int)
            else:
                assert val[j - 1] == rec[j]


@pytest.mark.django_db
def test_copy_to_table_file_error():
    with get_wrapped_connection() as db:
        with db.cursor() as cur:
            cur.execute(
                """
create table eek (
    id serial primary key,
    label text not null,
    data jsonb,
    lista int[],
    created_ts timestamptz
)
;
                """
            )

        cols = ["label", "data", "created_ts"]
        vals = [
            [
                "label-1",
                {"type": "rulebook", "data": {"ruleset": "ruleset-1"}},
                None,
                datetime.datetime.now(tz=datetime.timezone.utc),
            ],
            [
                "label-2",
                None,
                [1, 2, 3, 4],
                datetime.datetime.now(tz=datetime.timezone.utc),
            ],
        ]

        copy_file = StringIO()
        for val in vals:
            print(copyfy.copyfy_values(val), file=copy_file)  # noqa:T201
        copy_file.seek(0)

        with pytest.raises(DataError):
            copyfy.copy_to_table(
                db,
                "eek",
                cols,
                copy_file,
            )

        db.rollback()

        with db.cursor() as cur:
            cur.execute(
                """
drop table if exists eek;
                """
            )


@pytest.mark.django_db
def test_copy_to_table_integrity_error():
    with get_wrapped_connection() as db:
        with db.cursor() as cur:
            cur.execute(
                """
create table eek (
    id serial primary key,
    label text not null,
    data jsonb,
    lista int[],
    created_ts timestamptz
)
;
            """
            )

        cols = ["label", "data", "lista", "created_ts"]
        vals = [
            [
                None,
                {"type": "rulebook", "data": {"ruleset": "ruleset-1"}},
                None,
                datetime.datetime.now(tz=datetime.timezone.utc),
            ],
            [
                "label-2",
                None,
                [1, 2, 3, 4],
                datetime.datetime.now(tz=datetime.timezone.utc),
            ],
        ]

        copy_file = StringIO()
        for val in vals:
            print(copyfy.copyfy_values(val), file=copy_file)  # noqa:T201
        copy_file.seek(0)

        with pytest.raises(DatabaseError):
            copyfy.copy_to_table(
                db,
                "eek",
                cols,
                copy_file,
            )

        db.rollback()

        with db.cursor() as cur:
            cur.execute(
                """
drop table if exists eek;
                """
            )


def test_copyfy_model():
    kwargs = {
        "name": "proj-1",
        "description": "test project",
    }
    p = Project(**kwargs)
    p_mog = copyfy.copyfy(p)
    p_mog_values = p_mog.split(copyfy.DEFAULT_SEP)
    assert len(p_mog_values) == len(p._meta.concrete_fields)


def test_copyfy_model_with_sep():
    kwargs = {
        "name": "proj-1",
        "description": "test project",
    }
    p = Project(**kwargs)
    p_mog = copyfy.copyfy(p, sep="|")
    p_mog_values = p_mog.split("|")
    assert len(p_mog_values) == len(p._meta.concrete_fields)


def test_copyfy_model_with_fields():
    kwargs = {
        "name": "proj-1",
        "description": "test project",
    }
    p = Project(**kwargs)
    mog_fields = ["name"]
    p_mog = copyfy.copyfy(p, fields=mog_fields)
    p_mog_values = p_mog.split(copyfy.DEFAULT_SEP)
    assert len(p_mog_values) == len(mog_fields)
    assert kwargs["description"] not in p_mog
