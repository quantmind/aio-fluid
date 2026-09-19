import asyncio
from datetime import datetime
from typing import cast

import pytest
from sqlalchemy import func, text

from examples.db.tables1 import TaskType
from fluid.db import CrudDB
from fluid.utils.waiter import wait_for

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_upsert(db: CrudDB) -> None:
    table = db.tables["tasks"]
    task = await db.db_upsert(
        table, dict(unique_title="example"), dict(title="Example", severity=4)
    )
    assert task.id
    assert task.severity == 4
    assert task.done is None
    task2 = await db.db_upsert(
        table,
        dict(unique_title="example"),
        dict(title="Example", done=datetime.now()),
    )
    assert task2.id == task.id
    assert task2.done
    assert task2.severity == 4
    assert task2.created == task.created
    assert await db.db_count(table, dict(unique_title="example")) == 1


async def test_upsert_no_data(db: CrudDB) -> None:
    table = db.tables["multi_key_unique"]
    row = await db.db_upsert(table, dict(x=1, y=2))
    assert (row.x, row.y) == (1, 2)
    row2 = await db.db_upsert(table, dict(x=1, y=2))
    assert (row2.x, row2.y) == (1, 2)
    assert await db.db_count(table, dict(x=1, y=2)) == 1


async def test_upsert_concurrent(db: CrudDB) -> None:
    table = db.tables["tasks"]
    filters = dict(unique_title="concurrent")

    async def upsert_is_waiting_on_lock() -> bool:
        async with db.ensure_connection() as conn:
            result = await conn.execute(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' "
                    "AND query ILIKE '%INSERT INTO tasks%'"
                )
            )
            return cast(int, result.scalar()) > 0

    # the first upsert inserts the row but does not commit, so the second
    # upsert cannot see it and must wait on the unique index
    async with db.transaction() as conn:
        first = await db.db_upsert(
            table, filters, dict(title="Concurrent", severity=1), conn=conn
        )
        second_task = asyncio.create_task(
            db.db_upsert(table, filters, dict(title="Concurrent", severity=2))
        )
        await wait_for(upsert_is_waiting_on_lock, timeout=5)
    second = await second_task
    assert second.id == first.id
    assert second.severity == 2
    assert await db.db_count(table, filters) == 1


async def test_upsert_invalid_filters(db: CrudDB) -> None:
    table = db.tables["tasks"]
    with pytest.raises(ValueError):
        await db.db_upsert(table, {}, dict(title="Invalid"))
    with pytest.raises(ValueError):
        await db.db_upsert(table, {"severity:gt": 1}, dict(title="Invalid"))


async def test_upsert_many(db: CrudDB) -> None:
    table = db.tables["tasks"]
    rows = await db.db_upsert_many(
        table,
        [
            dict(unique_title="many-1", title="Many 1", severity=1),
            dict(unique_title="many-2", title="Many 2", severity=2),
        ],
        key=("unique_title",),
    )
    assert len(rows) == 2
    assert len({row.id for row in rows}) == 2
    ids = {row.unique_title: row.id for row in rows}
    rows = await db.db_upsert_many(
        table,
        [
            dict(unique_title="many-2", title="Many 2", severity=20),
            dict(unique_title="many-3", title="Many 3", severity=3),
        ],
        key=("unique_title",),
    )
    by_key = {row.unique_title: row for row in rows}
    assert by_key["many-2"].id == ids["many-2"]
    assert by_key["many-2"].severity == 20
    assert by_key["many-3"].severity == 3
    count = await db.db_count(table, {"unique_title": ["many-1", "many-2", "many-3"]})
    assert count == 3


async def test_upsert_many_batches(db: CrudDB) -> None:
    table = db.tables["multi_key_unique"]
    records = [dict(x=100, y=y) for y in range(10)]
    rows = await db.db_upsert_many(table, records, key=("x", "y"), batch_size=3)
    assert len(rows) == 10
    rows = await db.db_upsert_many(table, records, key=("x", "y"), batch_size=3)
    assert len(rows) == 10
    assert await db.db_count(table, dict(x=100)) == 10


async def test_upsert_many_empty(db: CrudDB) -> None:
    assert await db.db_upsert_many(db.tables["tasks"], [], key=("id",)) == []


async def test_upsert_many_invalid_records(db: CrudDB) -> None:
    table = db.tables["tasks"]
    key = ("unique_title",)
    with pytest.raises(ValueError, match="repeat the key"):
        await db.db_upsert_many(
            table,
            [
                dict(unique_title="dup", title="Dup 1"),
                dict(unique_title="dup", title="Dup 2"),
            ],
            key=key,
        )
    with pytest.raises(ValueError, match="same columns"):
        await db.db_upsert_many(
            table,
            [
                dict(unique_title="cols-1", title="Cols 1", severity=1),
                dict(unique_title="cols-2", title="Cols 2"),
            ],
            key=key,
        )
    with pytest.raises(ValueError, match="missing key columns"):
        await db.db_upsert_many(table, [dict(title="No key")], key=key)
    with pytest.raises(ValueError, match="not a column"):
        await db.db_upsert_many(table, [dict(title="Bad key")], key=("nope",))
    assert await db.db_count(table, {"unique_title": ["dup", "cols-1"]}) == 0


async def test_insert_missing_columns(db: CrudDB) -> None:
    table = db.tables["tasks"]
    result = await db.db_insert(
        table,
        [dict(title="Task1", severity=1), dict(title="Task2")],
    )
    rows = result.fetchall()
    assert len(rows) == 2
    titles = {r.title for r in rows}
    assert titles == {"Task1", "Task2"}
    severities = {r.title: r.severity for r in rows}
    assert severities["Task1"] == 1
    assert severities["Task2"] is None


async def test_select_columns(db: CrudDB) -> None:
    table = db.tables["tasks"]
    await db.db_insert(table, dict(title="SelectMe"))
    result = await db.db_select(
        table,
        dict(title="SelectMe"),
        columns=[table.c.title, func.lower(table.c.title).label("lower_title")],
    )
    row = result.one()
    assert row._fields == ("title", "lower_title")
    assert row.lower_title == "selectme"


async def test_search_query(db: CrudDB) -> None:
    table = db.tables["tasks"]
    await db.db_insert(table, dict(title="SearchMe", random="foo"))
    await db.db_insert(table, dict(title="IgnoreMe", random="bar"))
    # match on title
    sql = db.search_query(table, table.select(), ("title",), "SearchMe")
    async with db.ensure_connection() as conn:
        rows = (await conn.execute(sql)).fetchall()
    assert all("SearchMe" in r.title for r in rows)
    assert not any("IgnoreMe" in r.title for r in rows)
    # no-op when search string is empty
    sql_noop = db.search_query(table, table.select(), ("title",), "")
    async with db.ensure_connection() as conn:
        all_rows = (await conn.execute(sql_noop)).fetchall()
    assert len(all_rows) >= 2
    # no-op when search_fields is empty
    sql_noop2 = db.search_query(table, table.select(), (), "SearchMe")
    async with db.ensure_connection() as conn:
        all_rows2 = (await conn.execute(sql_noop2)).fetchall()
    assert len(all_rows2) >= 2


async def test_pool_pre_ping(db: CrudDB) -> None:
    # fill the pool, then drop every other pooled connection server side, as
    # a restart would
    async with db.connection(), db.connection(), db.connection():
        pass
    async with db.connection() as conn:
        await conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                " WHERE application_name = :app AND pid <> pg_backend_pid()"
            ),
            dict(app=db.app_name),
        )
    for _ in range(db.pool_size):
        async with db.connection() as conn:
            assert (await conn.execute(text("SELECT 1"))).scalar() == 1


async def test_delete(db: CrudDB) -> None:
    table = db.tables["tasks"]
    task = (await db.db_insert(table, dict(title="ToDelete"))).one()
    assert await db.db_count(table, dict(title="ToDelete")) == 1
    result = await db.db_delete(table, dict(title="ToDelete"))
    deleted = result.fetchall()
    assert len(deleted) == 1
    assert deleted[0].id == task.id
    assert await db.db_count(table, dict(title="ToDelete")) == 0


async def test_filter_many_values(db: CrudDB) -> None:
    # more values than a query can carry as separate parameters
    table = db.tables["tasks"]
    await db.db_insert(table, dict(title="ManyValues"))
    titles = ["ManyValues", *(f"missing-{i}" for i in range(40000))]
    assert await db.db_count(table, {"title": titles}) == 1
    assert await db.db_count(table, {"title": titles[1:]}) == 0
    assert (
        await db.db_count(table, {"title": "ManyValues", "title:ne": titles[1:]}) == 1
    )
    assert await db.db_count(table, {"title": "ManyValues", "title:ne": titles}) == 0


async def test_filter_values_enum(db: CrudDB) -> None:
    table = db.tables["tasks"]
    await db.db_insert(table, dict(title="EnumValues", type=TaskType.issue))
    filters = dict(title="EnumValues")
    assert await db.db_count(table, {**filters, "type": [TaskType.issue]}) == 1
    assert await db.db_count(table, {**filters, "type": [TaskType.todo]}) == 0
    assert await db.db_count(table, {**filters, "type:ne": [TaskType.todo]}) == 1


async def test_filter_values_empty(db: CrudDB) -> None:
    # an empty list matches nothing, and excludes nothing
    table = db.tables["tasks"]
    await db.db_insert(table, dict(title="EmptyValues"))
    assert await db.db_count(table, {"title": "EmptyValues", "id": []}) == 0
    assert await db.db_count(table, {"title": "EmptyValues", "id:ne": []}) == 1
