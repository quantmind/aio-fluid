from datetime import date

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from fluid.db import CrudDB
from fluid.db.crud import check_upsert_records
from fluid.utils.errors import ValidationError

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest.fixture(scope="module")
async def severities(db: CrudDB) -> list[int]:
    table = db.tables["tasks"]
    values = [1, 2, 3, 4, 5]
    await db.db_insert(
        table,
        [dict(title=f"Filter{s}", created_by="filters", severity=s) for s in values],
    )
    return values


async def select_severities(db: CrudDB, **filters: object) -> list[int]:
    result = await db.db_select(
        db.tables["tasks"], {"created_by": "filters", **filters}, order_by=("severity",)
    )
    return [row.severity for row in result.all()]


async def test_update(db: CrudDB) -> None:
    table = db.tables["tasks"]
    await db.db_insert(
        table,
        [
            dict(title="Update1", created_by="update"),
            dict(title="Update2", created_by="other"),
        ],
    )
    result = await db.db_update(table, dict(created_by="update"), dict(severity=9))
    rows = result.all()
    assert [(r.title, r.severity) for r in rows] == [("Update1", 9)]
    result = await db.db_select(table, dict(title="Update2"))
    assert result.one().severity is None


async def test_update_no_match(db: CrudDB) -> None:
    table = db.tables["tasks"]
    result = await db.db_update(table, dict(title="nothing-here"), dict(severity=1))
    assert result.all() == []


async def test_select_order_by(db: CrudDB, severities: list[int]) -> None:
    table = db.tables["tasks"]
    result = await db.db_select(
        table, dict(created_by="filters"), order_by=("-severity",)
    )
    assert [row.severity for row in result.all()] == list(reversed(severities))


async def test_select_order_by_unknown_column_ignored(
    db: CrudDB, severities: list[int]
) -> None:
    table = db.tables["tasks"]
    result = await db.db_select(
        table, dict(created_by="filters"), order_by=("not_a_column", "severity")
    )
    assert [row.severity for row in result.all()] == severities


async def test_filter_operators(db: CrudDB, severities: list[int]) -> None:
    assert await select_severities(db, **{"severity:eq": 3}) == [3]
    assert await select_severities(db, **{"severity:ne": 3}) == [1, 2, 4, 5]
    assert await select_severities(db, **{"severity:gt": 3}) == [4, 5]
    assert await select_severities(db, **{"severity:ge": 3}) == [3, 4, 5]
    assert await select_severities(db, **{"severity:lt": 3}) == [1, 2]
    assert await select_severities(db, **{"severity:le": 3}) == [1, 2, 3]


async def test_filter_operators_string_values(
    db: CrudDB, severities: list[int]
) -> None:
    # integer columns accept string values, as they come from query strings
    assert await select_severities(db, **{"severity:gt": "4"}) == [5]


async def test_filter_ne_many_values(db: CrudDB, severities: list[int]) -> None:
    assert await select_severities(db, **{"severity:ne": [1, 5]}) == [2, 3, 4]


async def test_filter_comparison_with_list_uses_first_value(
    db: CrudDB, severities: list[int]
) -> None:
    assert await select_severities(db, **{"severity:ge": [4, 1]}) == [4, 5]


async def test_filter_invalid_value(db: CrudDB) -> None:
    with pytest.raises(ValidationError):
        await db.db_select(db.tables["tasks"], {"severity": "not-a-number"})


async def test_filter_date_column_from_string(db: CrudDB) -> None:
    task = (await db.db_insert(db.tables["tasks"], dict(title="Dates"))).one()
    table = db.tables["randoms"]
    await db.db_insert(
        table,
        [
            dict(randomdate=date(2001, 1, d), price=d, tenor="1d", task_id=task.id)
            for d in (1, 3)
        ],
    )
    result = await db.db_select(
        table,
        {"randomdate:ge": "2001-01-02", "randomdate:lt": "2001-02-01"},
    )
    assert [row.randomdate for row in result.all()] == [date(2001, 1, 3)]


async def test_filter_jsonb_contains(db: CrudDB) -> None:
    table = sa.Table("jsonb_docs", sa.MetaData(), sa.Column("doc", JSONB))
    clause = db.default_filter_column(table.c.doc, "eq", {"a": 1})
    sql = str(clause.compile(dialect=postgresql.dialect()))
    assert "@>" in sql


async def test_upsert_query_requires_records(db: CrudDB) -> None:
    table = db.tables["tasks"]
    with pytest.raises(ValueError, match="at least one record"):
        check_upsert_records(table, [], ["id"])


async def test_filter_unknown_operator_raises(
    db: CrudDB, severities: list[int]
) -> None:
    with pytest.raises(ValidationError) as exc:
        await select_severities(db, **{"severity:like": 3})
    assert exc.value.field == "severity"


async def test_filter_jsonb_unsupported_operator_raises(db: CrudDB) -> None:
    table = sa.Table("jsonb_docs", sa.MetaData(), sa.Column("doc", JSONB))
    with pytest.raises(ValidationError) as exc:
        db.get_query(table, table.select(), params={"doc:ne": {"a": 1}})
    assert exc.value.field == "doc"


async def test_filter_invalid_value_names_the_field(db: CrudDB) -> None:
    with pytest.raises(ValidationError) as exc:
        await db.db_select(db.tables["tasks"], {"severity:gt": "not-a-number"})
    assert exc.value.field == "severity"
    assert "cursor" not in str(exc.value)
