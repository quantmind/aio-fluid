# Async Database

The `fluid.db` module provides a simple asynchronous interface to interact with postgres databases. It is built on top of the [sqlalchemy](https://www.sqlalchemy.org/) and [asyncpg](https://github.com/MagicStack/asyncpg) libraries.

## Installation

To use the database module, you need to install the `db` extra, and optionally the `cli` extra if you want to use the command line interface for managing database migrations:

```bash
pip install aio-fluid[db,cli]
```

## Database

There are two database classes:

- Database — provides connection management, transactions, and migrations.
- CrudDB — extends Database with CRUD helpers for common query patterns.

Most applications should use CrudDB directly:

```python
from fluid.db import CrudDB

db = CrudDB("postgresql+asyncpg://postgres:postgres@localhost:5432/mydb")
```

Note the use of the `postgresql+asyncpg` driver in the connection string. This is required for the async engine.

You can also load the DSN from an environment variable (defaults to `DATABASE`):

```python
db = CrudDB.from_env()
```

## Register a Table

Register tables against the database's `metadata` so that migrations and CRUD helpers can discover them:

```python
import sqlalchemy as sa

articles = sa.Table(
    "articles",
    db.metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("title", sa.String(200), nullable=False),
    sa.Column("author", sa.String(100)),
    sa.Column("score", sa.Integer),
    sa.Column("published_at", sa.DateTime(timezone=True)),
)
```

## Migrations

The Migration object wraps Alembic and is obtained from the database:

```python
mig = db.migration()
```

### Create the database

```python
# creates the database if it doesn't exist yet; returns False if it already existed
mig.db_create()
```

### Apply migrations

```python
# upgrade to the latest revision (equivalent to `alembic upgrade heads`)
mig.upgrade("heads")
```

For quick setup in tests or development, you can create all tables directly from metadata without Alembic revision files:

```python
mig.create_all()
```

### Generate a new revision

```python
# auto-generate a revision by diffing metadata against the current schema
mig.revision("add score column", autogenerate=True)
```

## Connections and Transactions

Use `connection()` when you only need to read data:

```python
async with db.connection() as conn:
    result = await conn.execute(sa.text("SELECT 1"))
```

Use `transaction()` when you need to write data — it commits on exit and rolls back on exception:

```python
async with db.transaction() as conn:
    await conn.execute(articles.insert().values(title="Hello"))
```

The `ensure_connection` and `ensure_transaction` variants are useful in functions that may receive an existing connection from the caller, avoiding nested connections:

```python
async def insert_article(data: dict, conn=None):
    async with db.ensure_transaction(conn) as conn:
        await conn.execute(articles.insert().values(**data))
```

## CRUD Operations

CrudDB provides async helpers that cover the most common patterns.

### Insert

```python
# single row
row = (await db.db_insert(articles, {"title": "Hello", "score": 10})).one()

# multiple rows — missing columns are filled with None automatically
rows = (await db.db_insert(articles, [
    {"title": "Hello", "score": 10},
    {"title": "World"},
])).fetchall()
```

All insert operations return the inserted rows via `RETURNING *`.

### Select

```python
rows = (await db.db_select(articles, {"author": "alice"})).fetchall()
```

Pass `order_by` to sort results. Prefix a field name with `-` for descending order:

```python
rows = (await db.db_select(articles, {}, order_by=("-published_at",))).fetchall()
```

#### Filter operators

Filters use a `"field:op"` key syntax. The default operator is `eq`:

| Key                       | Meaning          |
| ------------------------- | ---------------- |
| `"score"` or `"score:eq"` | `score = value`  |
| `"score:ne"`              | `score != value` |
| `"score:gt"`              | `score > value`  |
| `"score:ge"`              | `score >= value` |
| `"score:lt"`              | `score < value`  |
| `"score:le"`              | `score <= value` |

Pass a list as the value to use `IN` / `NOT IN`:

```python
# WHERE score IN (5, 10, 15)
rows = (await db.db_select(articles, {"score": [5, 10, 15]})).fetchall()
```

### Update

```python
rows = (await db.db_update(articles, {"author": "alice"}, {"score": 99})).fetchall()
```

Returns all updated rows via `RETURNING *`.

### Upsert

`db_upsert` updates a single record if it exists, or inserts it if it does not:

```python
# update score if the row exists, otherwise insert it
row = await db.db_upsert(
    articles,
    {"title": "Hello"},   # lookup key
    {"score": 42},        # data to set
)
```

### Delete

```python
deleted = (await db.db_delete(articles, {"author": "alice"})).fetchall()
```

Returns all deleted rows via `RETURNING *`.

### Count

```python
n = await db.db_count(articles, {"author": "alice"})
```

## Pagination

Pagination implements cursor-based pagination on top of CrudDB. It fetches one extra row beyond the requested limit to determine whether a next page exists, then encodes a cursor that the client returns with the next request.

```python
from fluid.db import Pagination

# first page
rows, cursor = await Pagination.create(
    "published_at",
    "id",
    limit=20,
    filters={"author": "alice"},
    desc=True,
).execute(db, articles)

# next page — filters and limit are embedded in the cursor
if cursor:
    rows, cursor = await Pagination.create(
        "published_at",
        "id",
        cursor=cursor,
        desc=True,
    ).execute(db, articles)
```

When `cursor` is provided, the `filters` and `limit` arguments are ignored — they are decoded from the cursor itself, ensuring consistent pages even if the caller changes them between requests.

### The ordering must be unique

The ordering fields must uniquely identify a row. The cursor encodes only the values of those fields for the first row of the next page, and the next query resumes from that position, so ties are ambiguous: rows sharing the same ordering values can appear on two consecutive pages, and rows can be skipped altogether, because the database is free to order tied rows differently between the two queries.

`published_at` alone is not unique — two articles can be published at the same instant — which is why the examples above order by `("published_at", "id")`. Appending the primary key makes the ordering total and the cursor unambiguous. The same rule applies to the ordering used with full-text search below.

### Full-text search

Combine pagination with a full-text search across multiple columns:

```python
from fluid.db import Pagination
from fluid.db.pagination import Search

rows, cursor = await Pagination.create(
    "published_at",
    "id",
    limit=20,
    search=Search(search_fields=("title", "author"), search_text="fluid"),
    desc=True,
).execute(db, articles)
```
