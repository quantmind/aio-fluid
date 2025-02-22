# Async Database

The `fluid.db` module provides a simple asynchronous interface to interact with postgres databases. It is built on top of the [sqlalchemy](https://www.sqlalchemy.org/) and [asyncpg](https://github.com/MagicStack/asyncpg) libraries.

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
