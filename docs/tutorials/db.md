# Async Database

The `fluid.db` module provides a simple asynchronous interface to interact with postgres databases. It is built on top of the [sqlalchemy](https://www.sqlalchemy.org/) and [asyncpg](https://github.com/MagicStack/asyncpg) libraries.

## Installation

To use the database module, you need to install the `db` extra, and optionally the `cli` extra if you want to use the command line interface for managing database migrations:

```bash
pip install aio-fluid[db,cli]
```

## Database

Create a database container with the [Database][fluid.db.Database] class:

```python
from fluid.db import Database

db = Database("postgresql+asyncpg://postgres:postgres@localhost:5432/fluid")
```

Note the use of the `postgresql+asyncpg` driver in the connection string. This is required to use the asyncpg driver.


## Register a Table

To interact with a table, you need to register it with the database container:

```python
import sqlalchemy as sa

users = sa.Table(
    "users",
    db.metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String),
)
```
