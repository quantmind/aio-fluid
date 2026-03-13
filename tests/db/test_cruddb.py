from datetime import datetime

import pytest

from fluid.db import CrudDB

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_upsert(db: CrudDB) -> None:
    task = await db.db_upsert(
        db.tables["tasks"], dict(title="Example"), dict(severity=4)
    )
    assert task.id
    assert task.severity == 4
    assert task.done is None
    task2 = await db.db_upsert(
        db.tables["tasks"], dict(title="Example"), dict(done=datetime.now())
    )
    assert task2.id == task.id
    assert task2.done
    assert await db.db_count(db.tables["tasks"], {}) == 1


async def test_upsert_no_data(db: CrudDB) -> None:
    task = await db.db_upsert(db.tables["tasks"], dict(title="Example2"))
    assert task.id
    assert task.title == "Example2"


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


async def test_delete(db: CrudDB) -> None:
    table = db.tables["tasks"]
    task = await db.db_upsert(table, dict(title="ToDelete"))
    assert await db.db_count(table, dict(title="ToDelete")) == 1
    result = await db.db_delete(table, dict(title="ToDelete"))
    deleted = result.fetchall()
    assert len(deleted) == 1
    assert deleted[0].id == task.id
    assert await db.db_count(table, dict(title="ToDelete")) == 0
