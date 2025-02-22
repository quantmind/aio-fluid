# DB CLI

The [Database][fluid.db.Database] command line interface (CLI) is a tool for managing database migrations and other database operations. It requires to install the additional `cli` extra:

```bash
pip install aio-fluid[db,cli]
```

It can be imported from `fluid.db.cli`:

```python
from fluid.db.cli import DbGroup
```


::: fluid.db.cli.DbGroup
