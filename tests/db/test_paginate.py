import warnings
from datetime import timedelta
from decimal import Decimal

import pytest

from fluid import settings
from fluid.db import CrudDB, OrderBy, Pagination, Search
from fluid.db.cursor import Cursor
from fluid.settings import get_settings
from fluid.utils.dates import isoformat, utcnow
from fluid.utils.errors import ValidationError

pytestmark = pytest.mark.asyncio(loop_scope="module")

BY_CREATED = OrderBy(("created",))
BY_CREATED_DESC = OrderBy(("created",), desc=True)
BY_SEVERITY = OrderBy(("severity",))


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
        BY_CREATED_DESC,
        filters={"created:ge": dates[1] + timedelta(seconds=30)},
        limit=5,
    )
    assert p.sort_by == "created"
    assert p.desc is True
    rows, cursor = await p.execute(db, db.tables["tasks"])
    assert len(rows) == 5
    assert cursor
    p = Pagination.create(BY_CREATED_DESC, cursor=cursor)
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
    p = Pagination.create(BY_CREATED, limit=2, search=search)
    rows, cursor = await p.execute(db, table, columns=columns)
    assert [tuple(r._fields) for r in rows] == [("title", "created")] * 2
    assert cursor
    p = Pagination.create(BY_CREATED, cursor=cursor, search=search)
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
        sort_by="created",
        values=(isoformat(created - timedelta(days=1)),),
        limit=0,
        filters={},
        search_text="",
    ).encode()
    p = Pagination.create(BY_CREATED, cursor=forged, filters={"title": "Forged1"})
    assert p.filters == {"title": "Forged1"}
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Forged1"]
    assert not cursor


async def test_paginate_cursor_filters_kept() -> None:
    cursor = Cursor(
        sort_by="created",
        values=("2024-01-01T00:00:00+00:00",),
        limit=5,
        filters={"title": "a", "org_id": "other"},
        search_text="",
    ).encode()
    p = Pagination.create(BY_CREATED, cursor=cursor, filters={"org_id": "mine"})
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
    p = Pagination.create(BY_CREATED, limit=2, search=search)
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Signed0", "Signed1"]
    assert "." in cursor
    p = Pagination.create(BY_CREATED, cursor=cursor, search=search)
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Signed2"]
    assert not cursor
    unsigned = Cursor(
        sort_by="created",
        values=(isoformat(created),),
        limit=0,
        filters={},
        search_text="",
    ).encode()
    with pytest.raises(ValidationError):
        Pagination.create(BY_CREATED, cursor=unsigned)


async def test_paginate_sort_by(db: CrudDB) -> None:
    """A client picks one of the supported fields and the direction,
    and the next page follows the ordering in the cursor"""
    table = db.tables["tasks"]
    created = utcnow()
    data = [
        dict(title=f"Order{i}", created=created - timedelta(days=i)) for i in range(3)
    ]
    await db.db_insert(table, data)
    search = Search(("title",), "Order")
    order_by = OrderBy(("created", "title"), unique_fields=("id",), desc=True)
    p = Pagination.create(order_by, limit=2, search=search)
    assert p.order_by_fields == ("created", "id")
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Order0", "Order1"]
    p = Pagination.create(order_by, sort_by="-title", limit=2, search=search)
    assert p.order_by_fields == ("title", "id")
    assert p.desc is True
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Order2", "Order1"]
    # the cursor wins over the requested sort field
    p = Pagination.create(order_by, sort_by="created", cursor=cursor, search=search)
    assert p.sort_by == "title"
    assert p.desc is True
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["Order0"]
    assert not cursor
    p = Pagination.create(order_by, sort_by="title", search=search)
    assert p.desc is False


async def test_paginate_sort_by_not_supported() -> None:
    order_by = OrderBy(("created",), unique_fields=("id",))
    for sort_by in ("title", "-title", "id", "-id"):
        with pytest.raises(ValidationError) as exc:
            Pagination.create(order_by, sort_by=sort_by)
        assert exc.value.field == "sort_by"


async def test_paginate_requires_a_field() -> None:
    for order_by in ((OrderBy(()),), ()):
        with pytest.raises(ValueError):
            Pagination.create(*order_by)


async def test_paginate_legacy_fields_deprecated() -> None:
    with pytest.warns(DeprecationWarning, match="pass an OrderBy instead") as record:
        p = Pagination.create("created", "id", desc=True)
    assert len(record) == 1
    # the warning points at the caller of Pagination.create
    assert record[0].filename == __file__
    assert p.order_by == OrderBy(("created",), unique_fields=("id",), desc=True)
    assert p.sort_by == "created"
    assert p.desc is True
    assert p.order_by_fields == ("created", "id")
    with pytest.warns(DeprecationWarning):
        p = Pagination.create("created")
    assert p.order_by == OrderBy(("created",))
    assert p.desc is False


async def test_paginate_order_by_no_warning() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Pagination.create(BY_CREATED)


async def test_paginate_order_by_cannot_mix() -> None:
    for order_by in ((BY_CREATED, "id"), ("id", BY_CREATED), (BY_CREATED, BY_CREATED)):
        with pytest.raises(ValueError, match="Cannot mix"):
            Pagination.create(*order_by)


async def test_paginate_order_by_with_desc() -> None:
    for desc in (True, False):
        with pytest.raises(ValueError, match="Cannot specify desc"):
            Pagination.create(BY_CREATED, desc=desc)


async def test_paginate_legacy(db: CrudDB) -> None:
    """The legacy fields paginate as the OrderBy they map to"""
    table = db.tables["tasks"]
    created_by = "legacy"
    data = [
        dict(title=f"Legacy{i}", severity=i % 3, created_by=created_by)
        for i in range(5)
    ]
    await db.db_insert(table, data)
    filters = dict(created_by=created_by)
    expected = await paginate_all(
        db, OrderBy(("severity",), unique_fields=("id",), desc=True), filters
    )
    rows: list[tuple] = []
    with pytest.warns(DeprecationWarning):
        p = Pagination.create("severity", "id", filters=filters, limit=2, desc=True)
    while True:
        page, cursor = await p.execute(db, table)
        rows.extend((r.severity, r.id) for r in page)
        if not cursor:
            break
        with pytest.warns(DeprecationWarning):
            p = Pagination.create("severity", "id", cursor=cursor, desc=True)
    assert rows == expected


async def test_paginate_cursor_not_supported() -> None:
    order_by = OrderBy(("created",), unique_fields=("id",))
    for sort_by, values in (
        ("title", ("a", 1)),
        ("id", (1, 1)),
        ("created", ("2024-01-01T00:00:00+00:00",)),
        ("created", ("2024-01-01T00:00:00+00:00", 1, 2)),
    ):
        cursor = Cursor(
            sort_by=sort_by,
            values=values,
            limit=5,
            filters={},
            search_text="",
        ).encode()
        with pytest.raises(ValidationError) as exc:
            Pagination.create(order_by, cursor=cursor)
        assert exc.value.field == "cursor"


async def test_paginate_cursor_limit_capped() -> None:
    max_limit = settings.DEFAULT_PAGINATION_MAX_LIMIT
    for limit in (0, -1, max_limit + 1):
        cursor = Cursor(
            sort_by="created",
            values=("2024-01-01T00:00:00+00:00",),
            limit=limit,
            filters={},
            search_text="",
        ).encode()
        assert Pagination.create(BY_CREATED, cursor=cursor).limit == max_limit


async def test_paginate_limit_capped() -> None:
    max_limit = settings.DEFAULT_PAGINATION_MAX_LIMIT
    default_limit = settings.DEFAULT_PAGINATION_LIMIT
    for limit, expected in (
        (None, default_limit),
        (0, default_limit),
        (1, 1),
        (max_limit, max_limit),
        (max_limit + 1, max_limit),
    ):
        assert Pagination.create(BY_CREATED, limit=limit).limit == expected


async def test_paginate_negative_limit() -> None:
    with pytest.raises(ValidationError) as exc:
        Pagination.create(BY_CREATED, limit=-1)
    assert exc.value.field == "limit"


async def test_paginate_limit_bounds_query(db: CrudDB) -> None:
    table = db.tables["tasks"]
    query = Pagination.create(BY_CREATED, limit=10**9).query(db, table)
    max_limit = settings.DEFAULT_PAGINATION_MAX_LIMIT
    assert query._limit == max_limit + 1


async def paginate_all(
    db: CrudDB, order_by: OrderBy, filters: dict, field: str = "severity"
) -> list[tuple]:
    table = db.tables["tasks"]
    pages = []
    p = Pagination.create(order_by, filters=filters, limit=2)
    while True:
        rows, cursor = await p.execute(db, table)
        pages.append([(getattr(r, field), r.id) for r in rows])
        if not cursor:
            break
        p = Pagination.create(order_by, cursor=cursor)
    return [row for page in pages for row in page]


@pytest.mark.parametrize("desc", [False, True])
async def test_paginate_unique_fields(db: CrudDB, desc: bool) -> None:
    """Rows tied on the sort field are split across pages without
    repeating or missing any of them"""
    table = db.tables["tasks"]
    created_by = f"multi-{desc}"
    data = [
        dict(title=f"Multi{i}", severity=i % 3, created_by=created_by) for i in range(7)
    ]
    await db.db_insert(table, data)
    filters = dict(created_by=created_by)
    order_by = ("-severity", "-id") if desc else ("severity", "id")
    expected = [
        (r.severity, r.id)
        for r in (await db.db_select(table, filters, order_by=order_by)).all()
    ]
    assert len(expected) == 7
    rows = await paginate_all(
        db, OrderBy(("severity",), unique_fields=("id",), desc=desc), filters
    )
    assert rows == expected


async def test_paginate_cursor_invalid_value(db: CrudDB) -> None:
    cursor = Cursor(
        sort_by="severity",
        values=("not-a-number",),
        limit=5,
        filters={},
        search_text="",
    ).encode()
    p = Pagination.create(BY_SEVERITY, cursor=cursor)
    with pytest.raises(ValidationError) as exc:
        await p.execute(db, db.tables["tasks"])
    assert exc.value.field == "cursor"


@pytest.mark.parametrize("desc", [False, True])
async def test_paginate_nulls_last(db: CrudDB, desc: bool) -> None:
    """A nullable sort field puts its NULL rows last in both directions, and
    the cursor resumes both into the NULL rows and among them"""
    table = db.tables["tasks"]
    created_by = f"nulls-{desc}"
    severities = [2, None, 0, None, 1, 2, None]
    await db.db_insert(
        table,
        [
            dict(title=f"Nulls{i}", severity=severity, created_by=created_by)
            for i, severity in enumerate(severities)
        ],
    )
    filters = dict(created_by=created_by)
    order_by = ("-severity", "-id") if desc else ("severity", "id")
    expected = [
        (r.severity, r.id)
        for r in (await db.db_select(table, filters, order_by=order_by)).all()
    ]
    assert [severity for severity, _ in expected] == [
        *sorted((s for s in severities if s is not None), reverse=desc),
        None,
        None,
        None,
    ]
    rows = await paginate_all(
        db, OrderBy(("severity",), unique_fields=("id",), desc=desc), filters
    )
    assert rows == expected


@pytest.mark.parametrize("desc", [False, True])
async def test_paginate_numeric_nulls_last(db: CrudDB, desc: bool) -> None:
    """A numeric sort field resumes from the decimal the cursor carries"""
    table = db.tables["tasks"]
    created_by = f"numeric-nulls-{desc}"
    points = [Decimal("1.5"), None, Decimal("0.25"), Decimal("3"), None]
    await db.db_insert(
        table,
        [
            dict(title=f"Points{i}", story_points=value, created_by=created_by)
            for i, value in enumerate(points)
        ],
    )
    rows = await paginate_all(
        db,
        OrderBy(("story_points",), unique_fields=("id",), desc=desc),
        dict(created_by=created_by),
        field="story_points",
    )
    assert [value for value, _ in rows] == [
        *sorted((p for p in points if p is not None), reverse=desc),
        None,
        None,
    ]


async def test_paginate_nulls_last_without_unique_fields(db: CrudDB) -> None:
    """A cursor resuming from a NULL value with no unique fields moves on to
    the NULL rows"""
    table = db.tables["tasks"]
    filters = dict(created_by="nulls-no-unique")
    await db.db_insert(
        table,
        [
            dict(title="NoUnique0", severity=None, **filters),
            dict(title="NoUnique1", severity=1, **filters),
        ],
    )
    p = Pagination.create(BY_SEVERITY, filters=filters, limit=1)
    rows, cursor = await p.execute(db, table)
    assert [r.severity for r in rows] == [1]
    assert cursor
    p = Pagination.create(BY_SEVERITY, cursor=cursor)
    rows, cursor = await p.execute(db, table)
    assert [r.title for r in rows] == ["NoUnique0"]
    assert not cursor
