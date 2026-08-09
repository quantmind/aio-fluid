# Utils

## fluid.utils.lazy.LazyGroup

```python
LazyGroup(*, lazy_subcommands=None, **kwargs)
```

Bases: `Group`

A click Group that can lazily load subcommands

This class extends the click.Group class to allow for subcommands to be lazily loaded from a module path.

It is useful when you have a large number of subcommands that you don't want to load until they are actually needed.

Available with the `cli` extra dependencies.

| PARAMETER          | DESCRIPTION                                                                                                                                                       |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `lazy_subcommands` | A dictionary mapping command names to their import paths. This allows subcommands to be lazily loaded from the specified module paths. **TYPE:** \`dict[str, str] |
| `**kwargs`         | Additional keyword arguments passed to the click.Group initializer. **TYPE:** `Any` **DEFAULT:** `{}`                                                             |

Source code in `fluid/utils/lazy.py`

```python
def __init__(
    self,
    *,
    lazy_subcommands: Annotated[
        dict[str, str] | None,
        Doc("""
        A dictionary mapping command names to their import paths.

        This allows subcommands to be lazily loaded from the specified module paths.
        """),
    ] = None,
    **kwargs: Annotated[
        Any,
        Doc("""
        Additional keyword arguments passed to the click.Group initializer.
        """),
    ],
):
    super().__init__(**kwargs)
    self.lazy_subcommands = lazy_subcommands or {}
```

### lazy_subcommands

```python
lazy_subcommands = lazy_subcommands or {}
```

### list_commands

```python
list_commands(ctx)
```

Source code in `fluid/utils/lazy.py`

```python
def list_commands(self, ctx: click.Context) -> list[str]:
    commands = super().list_commands(ctx)
    commands.extend(self.lazy_subcommands)
    return sorted(commands)
```

### get_command

```python
get_command(ctx, cmd_name)
```

Source code in `fluid/utils/lazy.py`

```python
def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
    if cmd_name in self.lazy_subcommands:
        return self._lazy_load(cmd_name)
    return super().get_command(ctx, cmd_name)
```

## fluid.utils.log.config

```python
config(
    level=None,
    other_level=WARNING,
    app_names=None,
    log_handler=None,
    log_format=None,
    formatters=None,
)
```

Configure logging for the application

| PARAMETER     | DESCRIPTION                                                                                                                                           |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `level`       | Log levels for application loggers defined by the app_names parameter. By default this value is taken from the LOG_LEVEL env variable **TYPE:** \`str |
| `other_level` | log levels for loggers not prefixed by app_names **TYPE:** \`str                                                                                      |
| `app_names`   | Application names for which the log level is set, these are the prefixes which will be set at log_level **TYPE:** \`Sequence[str]                     |
| `log_handler` | Log handler to use, by default it is taken from the LOG_HANDLER env variable and if missing plain is used **TYPE:** \`str                             |
| `log_format`  | log format to use, by default it is taken from the PYTHON_LOG_FORMAT env variable **TYPE:** \`str                                                     |
| `formatters`  | Additional formatters to add to the logging configuration **TYPE:** \`dict\[str, dict[str, str]\]                                                     |

Source code in `fluid/utils/log.py`

```python
def config(
    level: Annotated[
        str | int | None,
        Doc(
            "Log levels for application loggers defined by the `app_names` parameter. "
            "By default this value is taken from the `LOG_LEVEL` env variable"
        ),
    ] = None,
    other_level: Annotated[
        str | int,
        Doc("log levels for loggers not prefixed by `app_names`"),
    ] = logging.WARNING,
    app_names: Annotated[
        Sequence[str] | None,
        Doc(
            "Application names for which the log level is set, "
            "these are the prefixes which will be set at `log_level`"
        ),
    ] = None,
    log_handler: Annotated[
        str | None,
        Doc(
            "Log handler to use, by default it is taken from the "
            "`LOG_HANDLER` env variable and if missing `plain` is used"
        ),
    ] = None,
    log_format: Annotated[
        str | None,
        Doc(
            "log format to use, by default it is taken from the "
            "`PYTHON_LOG_FORMAT` env variable"
        ),
    ] = None,
    formatters: Annotated[
        dict[str, dict[str, str]] | None,
        Doc("Additional formatters to add to the logging configuration"),
    ] = None,
) -> dict:
    """Configure logging for the application"""
    cfg = _log_config(
        level=level,
        other_level=other_level,
        app_names=app_names,
        log_handler=log_handler,
        log_format=log_format,
        formatters=formatters,
    )
    dictConfig(cfg)
    return cfg
```
