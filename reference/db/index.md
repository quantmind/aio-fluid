# Database

It can be imported from `fluid.db`:

It requires the `db` extra to be installed:

```bash
pip install aio-fluid[db]
```

```python
from fluid.db import Database
```

## fluid.db.Database

```python
Database(
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

A container for tables in a database and a manager of asynchronous connections to a postgresql database

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
