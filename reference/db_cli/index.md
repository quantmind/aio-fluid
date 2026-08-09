# DB CLI

The Database command line interface (CLI) is a tool for managing database migrations and other database operations.

It requires to install the additional `cli` extra:

```bash
pip install aio-fluid[db,cli]
```

It can be imported from `fluid.db.cli`:

```python
from fluid.db.cli import DbGroup
```

## fluid.db.cli.DbGroup

```python
DbGroup(
    db,
    name="db",
    help="Manage database and migrations",
    **kwargs
)
```

Bases: `Group`

A click group for database commands

This class provides a CLI for a Database Application.

It requires to install the `cli` extra dependencies.

Source code in `fluid/db/cli.py`

```python
def __init__(
    self,
    db: Database,
    name: str = "db",
    help: str = "Manage database and migrations",  # noqa: A002
    **kwargs: Any,
) -> None:
    super().__init__(name=name, help=help, **kwargs)
    self.db = db
    for command in _db.commands.values():
        self.add_command(command)
```

### db

```python
db = db
```

### get_command

```python
get_command(ctx, name)
```

Source code in `fluid/db/cli.py`

```python
def get_command(self, ctx: click.Context, name: str) -> Optional[click.Command]:
    ctx.obj = {"db": self.db}
    return super().get_command(ctx, name)
```

### list_commands

```python
list_commands(ctx)
```

Source code in `fluid/db/cli.py`

```python
def list_commands(self, ctx: click.Context) -> list[str]:
    ctx.obj = {"db": self.db}
    return super().list_commands(ctx)
```
