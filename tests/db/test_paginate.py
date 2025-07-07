from datetime import timedelta

import pytest

from fluid.db import CrudDB, Pagination
from fluid.utils.dates import isoformat, utcnow

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_paginate(db: CrudDB) -> None:
    created = utcnow()
    dates = [created - timedelta(days=10 - i) for i in range(1, 11)]
    data = [
        dict(title=f"Example{i}", created=created - timedelta(days=10 - i))
        for i in range(1, 11)
    ]
    result = await db.db_insert(db.tables["tasks"], data)
    rows = result.all()
    assert len(rows) == 10
    p = Pagination.create(
        "created",
        filters={"created:ge": dates[1] + timedelta(seconds=30)},
        limit=5,
        desc=True,
    )
    rows, cursor = await p.execute(db, db.tables["tasks"])
    assert len(rows) == 5
    assert cursor
    p = Pagination.create(
        "created",
        cursor=cursor,
        desc=True,
    )
    extra_rows, cursor = await p.execute(db, db.tables["tasks"])
    assert len(extra_rows) == 3
    all_rows = list(rows)
    all_rows.extend(extra_rows)
    assert not cursor
    assert [isoformat(r.created) for r in all_rows] == [
        isoformat(d) for d in reversed(dates[2:])
    ]
