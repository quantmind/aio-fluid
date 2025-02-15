# Async Database

The `fluid.db` module provides a simple asynchronous interface to interact with postgres databases. It is built on top of the [sqlalchemy](https://www.sqlalchemy.org/) and [asyncpg](https://github.com/MagicStack/asyncpg) libraries.

## Database

Create a database container with the `Database` class:

```python
from fluid.db import Database

db = Database("postgresql+asyncpg://postgres:postgres@localhost:5432/fluid")
```

Note the use of the `postgresql+asyncpg` driver in the connection string. THis is required to use the asyncpg driver.
