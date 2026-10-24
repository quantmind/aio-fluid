from datetime import timedelta

import pytest

from fluid import settings
from fluid.db import CrudDB, Pagination, Search
from fluid.db.cursor import Cursor, CursorEntry
from fluid.settings import get_settings
from fluid.utils.dates import isoformat, utcnow
from fluid.utils.errors import ValidationError

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


async def test_paginate_columns(db: CrudDB) -> None:
    table = db.tables["tasks"]
    created = utcnow()
    data = [
        dict(title=f"Columns{i}", created=created + timedelta(days=i)) for i in range(3)
    ]
    await db.db_insert(table, data)
    columns = [table.c.title, table.c.created]
    search = Search(("title",), "Columns")
    p = Pagination.create("created", limit=2, search=search)
    rows, cursor = await p.execute(db, table, columns=columns)
    assert [tuple(r._fields) for r in rows] == [("title", "created")] * 2
    assert cursor
    p = Pagination.create("created", cursor=cursor, search=search)
    rows, cursor = await p.execute(db, table, columns=columns)
    assert [r.title for r in rows] == ["Columns2"]
    assert not cursor


async def test_paginate_cursor_cannot_drop_filters(db: CrudDB) -> None:
    table = db.tables["tasks"]
    created = utcnow()
    data = [
        dict(title=f"Forged{i}", created=created + timedelta(days=i)) for i in range(3)
    ]
    await db.db_insert(table, data)
    forged = Cursor(
        entries=(CursorEntry("created", isoformat(created - timedelta(days=1))),),
        limit=0,
        filters={},
        search_text="",
    ).encode()
    p = Pagination.create("created", cursor=forged, filters={"title": "Forged1"})
    assert p.filters == {"title": "Forged1"}
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Forged1"]
    assert not cursor


async def test_paginate_cursor_filters_kept() -> None:
    cursor = Cursor(
        entries=(CursorEntry("created", "2024-01-01T00:00:00+00:00"),),
        limit=5,
        filters={"title": "a", "org_id": "other"},
        search_text="",
    ).encode()
    p = Pagination.create("created", cursor=cursor, filters={"org_id": "mine"})
    assert p.filters == {"title": "a", "org_id": "mine"}
    assert p.limit == 5


async def test_paginate_signed_cursor(
    db: CrudDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "pagination_secret", "test-secret")
    table = db.tables["tasks"]
    created = utcnow()
    data = [
        dict(title=f"Signed{i}", created=created + timedelta(days=i)) for i in range(3)
    ]
    await db.db_insert(table, data)
    search = Search(("title",), "Signed")
    p = Pagination.create("created", limit=2, search=search)
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Signed0", "Signed1"]
    assert "." in cursor
    p = Pagination.create("created", cursor=cursor, search=search)
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Signed2"]
    assert not cursor
    unsigned = Cursor(
        entries=(CursorEntry("created", isoformat(created)),),
        limit=0,
        filters={},
        search_text="",
    ).encode()
    with pytest.raises(ValidationError):
        Pagination.create("created", cursor=unsigned)


async def test_paginate_cursor_limit_capped() -> None:
    max_limit = settings.DEFAULT_PAGINATION_MAX_LIMIT
    for limit in (0, -1, max_limit + 1):
        cursor = Cursor(
            entries=(CursorEntry("created", "2024-01-01T00:00:00+00:00"),),
            limit=limit,
            filters={},
            search_text="",
        ).encode()
        assert Pagination.create("created", cursor=cursor).limit == max_limit
