# CrudDB

The CrudDB class inherits from Database to provide standard CRUD operations for a database table.

It requires the `db` extra to be installed:

```bash
pip install aio-fluid[db]
```

It can be imported from `fluid.db`:

```python
from fluid.db import CrudDB
```

## fluid.db.CrudDB

```python
CrudDB(
    dsn,
    echo=(lambda: DBECHO)(),
    pool_size=(lambda: DBPOOL_MAX_SIZE)(),
    max_overflow=(lambda: DBPOOL_MAX_OVERFLOW)(),
    metadata=MetaData(),
    migration_path="",
    app_name=(lambda: APP_NAME)(),
    _engine=None,
)
```

Bases: `Database`

A Database with additional methods for CRUD operations

### dsn

```python
dsn
```

data source name, aka connection string

Example: `postgresql+asyncpg://user:password@localhost/dbname`

Note that the `+asyncpg` part is important for the async engine.

Currently, only `postgresql+asyncpg` is supported, but other databases may be supported in the future.

### echo

```python
echo = field(default_factory=lambda: settings.DBECHO)
```

Echo SQL queries to stdout

It defaults to the `DBECHO` setting in the settings module

### pool_size

```python
pool_size = field(
    default_factory=lambda: settings.DBPOOL_MAX_SIZE
)
```

### max_overflow

```python
max_overflow = field(
    default_factory=lambda: settings.DBPOOL_MAX_OVERFLOW
)
```

### metadata

```python
metadata = field(default_factory=sa.MetaData)
```

### migration_path

```python
migration_path = ''
```

Path to the directory containing migration files. If empty, migrations will be stored in the default location `migrations` in the current working directory.

### app_name

```python
app_name = field(default_factory=lambda: settings.APP_NAME)
```

### tables

```python
tables
```

A dictionary of tables in the database

### engine

```python
engine
```

The :class:`sqlalchemy.ext.asyncio.AsyncEngine` creating connection and transactions

### url

```python
url
```

The SQLAlchemy URL object for the database

### sync_engine

```python
sync_engine
```

The sqlalchemy Engine object for synchrouns operations

### from_env

```python
from_env(
    *,
    dsn=None,
    schema=None,
    migration_path=None,
    app_name=None,
    max_overflow=None,
    pool_size=None,
    db_name=None
)
```

Create a new database container from environment variables as defaults

Source code in `fluid/db/container.py`

```python
@classmethod
def from_env(
    cls,
    *,
    dsn: str | None = None,
    schema: str | None = None,
    migration_path: str | Path | None = None,
    app_name: str | None = None,
    max_overflow: int | None = None,
    pool_size: int | None = None,
    db_name: str | None = None,
) -> Self:
    """Create a new database container from environment variables as defaults"""
    if dsn is None:
        dsn = settings.DATABASE
    if schema is None:
        schema = settings.DATABASE_SCHEMA
    kwargs = compact_dict(
        migration_path=migration_path,
        app_name=app_name,
        max_overflow=max_overflow,
        pool_size=pool_size,
    )
    if db_name:
        dsn = (
            make_url(dsn)
            .set(database=db_name)
            .render_as_string(hide_password=False)
        )
    return cls(dsn=dsn, metadata=sa.MetaData(schema=schema), **kwargs)
```

### connection

```python
connection()
```

Context manager for obtaining an asynchronous connection

Source code in `fluid/db/container.py`

```python
@asynccontextmanager
async def connection(self) -> AsyncIterator[AsyncConnection]:
    """Context manager for obtaining an asynchronous connection"""
    async with self.engine.connect() as conn:
        yield conn
```

### ensure_connection

```python
ensure_connection(conn=None)
```

Context manager for obtaining an asynchronous connection

Source code in `fluid/db/container.py`

```python
@asynccontextmanager
async def ensure_connection(
    self,
    conn: AsyncConnection | None = None,
) -> AsyncIterator[AsyncConnection]:
    """Context manager for obtaining an asynchronous connection"""
    if conn:
        yield conn
    else:
        async with self.engine.connect() as conn:
            yield conn
```

### transaction

```python
transaction()
```

Context manager for initializing an asynchronous database transaction

Source code in `fluid/db/container.py`

```python
@asynccontextmanager
async def transaction(self) -> AsyncIterator[AsyncConnection]:
    """Context manager for initializing an asynchronous database transaction"""
    async with self.engine.begin() as conn:
        yield conn
```

### ensure_transaction

```python
ensure_transaction(conn=None)
```

Context manager for ensuring we a connection has initialized a database transaction

Source code in `fluid/db/container.py`

```python
@asynccontextmanager
async def ensure_transaction(
    self,
    conn: AsyncConnection | None = None,
) -> AsyncIterator[AsyncConnection]:
    """Context manager for ensuring we a connection has initialized
    a database transaction"""
    if conn:
        if not conn.in_transaction():
            async with conn.begin():
                yield conn
        else:
            yield conn
    else:
        async with self.transaction() as conn:
            yield conn
```

### close

```python
close()
```

Close the asynchronous db engine if opened

Source code in `fluid/db/container.py`

```python
async def close(self) -> None:
    """Close the asynchronous db engine if opened"""
    if self._engine is not None:
        engine, self._engine = self._engine, None
        await engine.dispose()
```

### ping

```python
ping()
```

Ping the database

Source code in `fluid/db/container.py`

```python
async def ping(self) -> str:
    """Ping the database"""
    # TODO: we need a custom ping query
    async with self.connection() as conn:
        await conn.execute(sa.text("SELECT 1"))
    return "ok"
```

### migration

```python
migration()
```

The migration manager for this database

Source code in `fluid/db/container.py`

```python
def migration(self) -> Migration:
    """The migration manager for this database"""
    return Migration(db=self)
```

### db_select

```python
db_select(table, filters, *, order_by=None, conn=None)
```

Select rows from a given table

| PARAMETER  | DESCRIPTION                                                                                                            |
| ---------- | ---------------------------------------------------------------------------------------------------------------------- |
| `table`    | The table to select from **TYPE:** `FromClause`                                                                        |
| `filters`  | Key-value pairs for filtering rows; supports 'field:op' syntax for operators (eq, ne, gt, ge, lt, le) **TYPE:** `dict` |
| `order_by` | Column names to order by; prefix with '-' for descending **TYPE:** \`tuple[str, ...]                                   |
| `conn`     | Optional existing connection to reuse **TYPE:** \`AsyncConnection                                                      |

Source code in `fluid/db/crud.py`

```python
async def db_select(
    self,
    table: Annotated[FromClause, Doc("The table to select from")],
    filters: Annotated[
        dict,
        Doc(
            "Key-value pairs for filtering rows; supports 'field:op' syntax "
            "for operators (eq, ne, gt, ge, lt, le)"
        ),
    ],
    *,
    order_by: Annotated[
        tuple[str, ...] | None,
        Doc("Column names to order by; prefix with '-' for descending"),
    ] = None,
    conn: Annotated[
        AsyncConnection | None, Doc("Optional existing connection to reuse")
    ] = None,
) -> CursorResult:
    """Select rows from a given table"""
    sql_query = self.get_query(table, Select(table), params=filters)
    if order_by:
        sql_query = self.order_by_query(table, cast(Select, sql_query), order_by)
    async with self.ensure_transaction(conn) as conn:
        return await conn.execute(sql_query)
```

### db_insert

```python
db_insert(table, data, *, conn=None)
```

Insert one or more rows into a table, returning the inserted rows

| PARAMETER | DESCRIPTION                                                                                                                 |
| --------- | --------------------------------------------------------------------------------------------------------------------------- |
| `table`   | The table to insert into **TYPE:** `Table`                                                                                  |
| `data`    | A single row dict or a list of row dicts; missing columns in a multi-row insert are filled with None **TYPE:** \`list[dict] |
| `conn`    | Optional existing connection to reuse **TYPE:** \`AsyncConnection                                                           |

Source code in `fluid/db/crud.py`

```python
async def db_insert(
    self,
    table: Annotated[Table, Doc("The table to insert into")],
    data: Annotated[
        list[dict] | dict,
        Doc(
            "A single row dict or a list of row dicts; missing columns in "
            "a multi-row insert are filled with None"
        ),
    ],
    *,
    conn: Annotated[
        AsyncConnection | None, Doc("Optional existing connection to reuse")
    ] = None,
) -> CursorResult:
    """Insert one or more rows into a table, returning the inserted rows"""
    async with self.ensure_transaction(conn) as conn:
        sql_query = self.insert_query(table, data)
        return await conn.execute(sql_query)
```

### db_update

```python
db_update(table, filters, data, *, conn=None)
```

Update rows matching the filters, returning all updated rows

| PARAMETER | DESCRIPTION                                                                             |
| --------- | --------------------------------------------------------------------------------------- |
| `table`   | The table to update **TYPE:** `Table`                                                   |
| `filters` | Key-value pairs identifying rows to update; supports 'field:op' syntax **TYPE:** `dict` |
| `data`    | Column values to set on the matching rows **TYPE:** `dict`                              |
| `conn`    | Optional existing connection to reuse **TYPE:** \`AsyncConnection                       |

Source code in `fluid/db/crud.py`

```python
async def db_update(
    self,
    table: Annotated[Table, Doc("The table to update")],
    filters: Annotated[
        dict,
        Doc(
            "Key-value pairs identifying rows to update; supports 'field:op' syntax"
        ),
    ],
    data: Annotated[dict, Doc("Column values to set on the matching rows")],
    *,
    conn: Annotated[
        AsyncConnection | None, Doc("Optional existing connection to reuse")
    ] = None,
) -> CursorResult:
    """Update rows matching the filters, returning all updated rows"""
    update = (
        cast(
            Update,
            self.get_query(table, table.update(), params=filters),
        )
        .values(**data)
        .returning(*table.columns)
    )
    async with self.ensure_transaction(conn) as conn:
        return await conn.execute(update)
```

### db_upsert

```python
db_upsert(table, filters, data=None, *, conn=None)
```

Update a single row if it exists, otherwise insert it, returning the row

| PARAMETER | DESCRIPTION                                                                                           |
| --------- | ----------------------------------------------------------------------------------------------------- |
| `table`   | The table to upsert into **TYPE:** `Table`                                                            |
| `filters` | Key-value pairs used to look up the existing row **TYPE:** `dict`                                     |
| `data`    | Column values to set; if None, the row is fetched or inserted using only the filters **TYPE:** \`dict |
| `conn`    | Optional existing connection to reuse **TYPE:** \`AsyncConnection                                     |

Source code in `fluid/db/crud.py`

```python
async def db_upsert(
    self,
    table: Annotated[Table, Doc("The table to upsert into")],
    filters: Annotated[
        dict, Doc("Key-value pairs used to look up the existing row")
    ],
    data: Annotated[
        dict | None,
        Doc(
            "Column values to set; if None, the row is fetched or inserted "
            "using only the filters"
        ),
    ] = None,
    *,
    conn: Annotated[
        AsyncConnection | None, Doc("Optional existing connection to reuse")
    ] = None,
) -> Row:
    """Update a single row if it exists, otherwise insert it, returning the row"""
    if data:
        result = await self.db_update(table, filters, data, conn=conn)
    else:
        result = await self.db_select(table, filters, conn=conn)
    record = result.one_or_none()
    if record is None:
        insert_data = data.copy() if data else {}
        insert_data.update(filters)
        result = await self.db_insert(table, insert_data, conn=conn)
        record = result.one()
    return record
```

### db_delete

```python
db_delete(table, filters, *, conn=None)
```

Delete rows matching the filters, returning the deleted rows

| PARAMETER | DESCRIPTION                                                                             |
| --------- | --------------------------------------------------------------------------------------- |
| `table`   | The table to delete from **TYPE:** `Table`                                              |
| `filters` | Key-value pairs identifying rows to delete; supports 'field:op' syntax **TYPE:** `dict` |
| `conn`    | Optional existing connection to reuse **TYPE:** \`AsyncConnection                       |

Source code in `fluid/db/crud.py`

```python
async def db_delete(
    self,
    table: Annotated[Table, Doc("The table to delete from")],
    filters: Annotated[
        dict,
        Doc(
            "Key-value pairs identifying rows to delete; supports 'field:op' syntax"
        ),
    ],
    *,
    conn: Annotated[
        AsyncConnection | None, Doc("Optional existing connection to reuse")
    ] = None,
) -> CursorResult:
    """Delete rows matching the filters, returning the deleted rows"""
    sql_query = self.get_query(
        table,
        table.delete().returning(*table.columns),
        params=filters,
    )
    async with self.ensure_transaction(conn) as conn:
        return await conn.execute(sql_query)
```

### db_count

```python
db_count(table, filters, *, conn=None)
```

Count rows in a table matching the given filters

| PARAMETER | DESCRIPTION                                                                     |
| --------- | ------------------------------------------------------------------------------- |
| `table`   | The table to count rows in **TYPE:** `FromClause`                               |
| `filters` | Key-value pairs for filtering rows; supports 'field:op' syntax **TYPE:** `dict` |
| `conn`    | Optional existing connection to reuse **TYPE:** \`AsyncConnection               |

Source code in `fluid/db/crud.py`

```python
async def db_count(
    self,
    table: Annotated[FromClause, Doc("The table to count rows in")],
    filters: Annotated[
        dict, Doc("Key-value pairs for filtering rows; supports 'field:op' syntax")
    ],
    *,
    conn: Annotated[
        AsyncConnection | None, Doc("Optional existing connection to reuse")
    ] = None,
) -> int:
    """Count rows in a table matching the given filters"""
    count_query = self.db_count_query(
        cast(
            Select,
            self.get_query(
                table,
                table.select(),
                params=filters,
            ),
        ),
    )
    async with self.ensure_connection(conn) as conn:
        result: CursorResult = await conn.execute(count_query)
        return cast(int, result.scalar())
```

### insert_query

```python
insert_query(table, records)
```

| PARAMETER | DESCRIPTION                                                     |
| --------- | --------------------------------------------------------------- |
| `table`   | The table to insert into **TYPE:** `Table`                      |
| `records` | A single row dict or a list of row dicts **TYPE:** \`list[dict] |

Source code in `fluid/db/crud.py`

```python
def insert_query(
    self,
    table: Annotated[Table, Doc("The table to insert into")],
    records: Annotated[
        list[dict] | dict, Doc("A single row dict or a list of row dicts")
    ],
) -> Insert:
    if isinstance(records, dict):
        records = [records]
    else:
        cols: Set[str] = set()
        for record in records:
            cols.update(record)
        new_records = []
        for record in records:
            if len(record) < len(cols):
                record = record.copy()
                missing = cols.difference(record)
                for col in missing:
                    record[col] = None
            new_records.append(record)
        records = new_records
    return insert(table).values(records).returning(*table.columns)
```

### get_query

```python
get_query(table, sql_query, *, params=None)
```

Apply filters from params to a SQLAlchemy query and return it

| PARAMETER   | DESCRIPTION                                                             |
| ----------- | ----------------------------------------------------------------------- |
| `table`     | The table the query targets **TYPE:** `FromClause`                      |
| `sql_query` | The base SQLAlchemy query to apply filters to **TYPE:** `QueryType`     |
| `params`    | Key-value filter pairs; keys may use 'field:op' syntax **TYPE:** \`dict |

Source code in `fluid/db/crud.py`

```python
def get_query(
    self,
    table: Annotated[FromClause, Doc("The table the query targets")],
    sql_query: Annotated[
        QueryType, Doc("The base SQLAlchemy query to apply filters to")
    ],
    *,
    params: Annotated[
        dict | None, Doc("Key-value filter pairs; keys may use 'field:op' syntax")
    ] = None,
) -> QueryType:
    """Apply filters from params to a SQLAlchemy query and return it"""
    filters: list = []
    columns = table.c
    params = params or {}

    for key, value in params.items():
        bits = key.split(":")
        field = bits[0]
        op = bits[1] if len(bits) == 2 else "eq"
        field = getattr(columns, field)
        result = self.default_filter_column(field, op, value)
        if result is not None:
            if not isinstance(result, (list, tuple)):
                result = (result,)
            filters.extend(result)
    if filters:
        whereclause = and_(*filters) if len(filters) > 1 else filters[0]
        sql_query = cast(Select, sql_query).where(whereclause)
    return sql_query
```

### db_count_query

```python
db_count_query(sql_query)
```

| PARAMETER   | DESCRIPTION                                                     |
| ----------- | --------------------------------------------------------------- |
| `sql_query` | The filtered SELECT query to wrap in a COUNT **TYPE:** `Select` |

Source code in `fluid/db/crud.py`

```python
def db_count_query(
    self,
    sql_query: Annotated[
        Select, Doc("The filtered SELECT query to wrap in a COUNT")
    ],
) -> Select:
    return select(func.count()).select_from(sql_query.alias("inner"))
```

### order_by_query

```python
order_by_query(table, sql_query, order_by)
```

Apply ordering to a SELECT query

| PARAMETER   | DESCRIPTION                                                                          |
| ----------- | ------------------------------------------------------------------------------------ |
| `table`     | The table the query targets **TYPE:** `FromClause`                                   |
| `sql_query` | The SELECT query to add ordering to **TYPE:** `Select`                               |
| `order_by`  | Column names to order by; prefix with '-' for descending **TYPE:** `tuple[str, ...]` |

Source code in `fluid/db/crud.py`

```python
def order_by_query(
    self,
    table: Annotated[FromClause, Doc("The table the query targets")],
    sql_query: Annotated[Select, Doc("The SELECT query to add ordering to")],
    order_by: Annotated[
        tuple[str, ...],
        Doc("Column names to order by; prefix with '-' for descending"),
    ],
) -> Select:
    """Apply ordering to a SELECT query"""
    return sql_query.order_by(*self.order_by_columns(table, order_by))
```

### order_by_columns

```python
order_by_columns(table, order_by)
```

Return a list of SQLAlchemy column expressions for the given order_by fields

| PARAMETER  | DESCRIPTION                                                                          |
| ---------- | ------------------------------------------------------------------------------------ |
| `table`    | The table whose columns are referenced **TYPE:** `FromClause`                        |
| `order_by` | Column names to order by; prefix with '-' for descending **TYPE:** `tuple[str, ...]` |

Source code in `fluid/db/crud.py`

```python
def order_by_columns(
    self,
    table: Annotated[FromClause, Doc("The table whose columns are referenced")],
    order_by: Annotated[
        tuple[str, ...],
        Doc("Column names to order by; prefix with '-' for descending"),
    ],
) -> list[Column]:
    """Return a list of SQLAlchemy column expressions for the given
    order_by fields"""
    columns = []
    for name in order_by:
        if name.startswith("-"):
            order_by_column = getattr(table.c, name[1:], None)
            if order_by_column is not None:
                columns.append(order_by_column.desc())
        else:
            order_by_column = getattr(table.c, name, None)
            if order_by_column is not None:
                columns.append(order_by_column)
    return columns
```

### search_query

```python
search_query(table, sql_query, search_fields, search)
```

Apply a case-insensitive substring search across the given columns

| PARAMETER       | DESCRIPTION                                                           |
| --------------- | --------------------------------------------------------------------- |
| `table`         | The table whose columns are searched **TYPE:** `FromClause`           |
| `sql_query`     | The SELECT query to add the search filter to **TYPE:** `Select`       |
| `search_fields` | Column names to search across using ILIKE **TYPE:** `tuple[str, ...]` |
| `search`        | Search text; empty string is a no-op **TYPE:** `str`                  |

Source code in `fluid/db/crud.py`

```python
def search_query(
    self,
    table: Annotated[FromClause, Doc("The table whose columns are searched")],
    sql_query: Annotated[
        Select, Doc("The SELECT query to add the search filter to")
    ],
    search_fields: Annotated[
        tuple[str, ...], Doc("Column names to search across using ILIKE")
    ],
    search: Annotated[str, Doc("Search text; empty string is a no-op")],
) -> Select:
    """Apply a case-insensitive substring search across the given columns"""
    if search and search_fields:
        columns = [getattr(table.c, col) for col in search_fields]
        return sql_query.where(or_(*(col.ilike(f"%{search}%") for col in columns)))
    return sql_query
```

### default_filter_column

```python
default_filter_column(column, op, value)
```

Build a SQLAlchemy WHERE clause expression for a single column filter

| PARAMETER | DESCRIPTION                                                               |
| --------- | ------------------------------------------------------------------------- |
| `column`  | The SQLAlchemy column to filter on **TYPE:** `Column`                     |
| `op`      | Comparison operator: eq, ne, gt, ge, lt, or le **TYPE:** `str`            |
| `value`   | Comparison value; a list triggers IN / NOT IN for eq / ne **TYPE:** `Any` |

Source code in `fluid/db/crud.py`

```python
def default_filter_column(
    self,
    column: Annotated[Column, Doc("The SQLAlchemy column to filter on")],
    op: Annotated[
        str, Doc("Comparison operator: `eq`, `ne`, `gt`, `ge`, `lt`, or `le`")
    ],
    value: Annotated[
        Any, Doc("Comparison value; a list triggers IN / NOT IN for `eq` / `ne`")
    ],
) -> Any:
    """Build a SQLAlchemy WHERE clause expression for a single column filter"""
    if isinstance(column.type, JSONB) and isinstance(value, dict):
        if op == "eq":
            return column.contains(value)
        return None

    if multiple := isinstance(value, (list, tuple)):
        value = tuple(column_value_to_python(column, v) for v in value)
    else:
        value = column_value_to_python(column, value)

    if multiple and op in ("eq", "ne"):
        if op == "eq":
            return column.in_(value)
        elif op == "ne":
            return ~column.in_(value)
    else:
        if multiple:
            assert len(value) > 0
            value = value[0]

        if op == "eq":
            return column == value
        elif op == "ne":
            return column != value
        elif op == "gt":
            return column > value
        elif op == "ge":
            return column >= value
        elif op == "lt":
            return column < value
        elif op == "le":
            return column <= value
```
