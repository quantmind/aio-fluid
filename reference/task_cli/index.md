# Task Manager CLI

Command line tools for TaskManager applications.

This module requires the `cli` extra to be installed:

```bash
pip install aio-fluid[cli]
```

## Setup

Wrap your FastAPI app with TaskManagerCLI and call it as the entry point:

```python
from fluid.scheduler.cli import TaskManagerCLI

task_manager_cli = TaskManagerCLI("examples.tasks:task_app")

if __name__ == "__main__":
    task_manager_cli()
```

`task_manager_app` can be a FastAPI instance, a callable that returns one, or a dotted-import string (resolved at call time so the CLI stays importable without loading the full app).

## Commands

### `ls` — list registered tasks

Prints a table of all registered tasks with their schedule, priority, timeout, CPU-bound flag, and tags.

```bash
python -m myapp ls
```

Pass `--tags`/`-t` (repeatable) to only list tasks that have at least one of the given tags:

```bash
python -m myapp ls --tags fast --tags slow
```

### `serve` — start the HTTP server

```bash
python -m myapp serve --host 0.0.0.0 --port 8080
```

Delegates to `uvicorn`. Pass `--reload` for development auto-reload.

### `enable` — enable or disable a task

```bash
python -m myapp enable <task-name>           # enable
python -m myapp enable <task-name> --disable  # disable
```

### `exec` — execute a task

Runs a registered task synchronously and prints the result table.

```bash
python -m myapp exec <task-name> [OPTIONS]
```

Each task exposes its params model fields as CLI options (via [pydanclick](https://github.com/felix-martel/pydanclick)):

```bash
python -m myapp exec add --a 5 --b 3
```

#### Passing parameters as JSON

All task parameters can also be supplied together as a JSON string with `--params`. Values in `--params` **always take priority** over individual CLI option defaults:

```bash
python -m myapp exec add --params '{"a": 5, "b": 3}'
```

This is useful when parameters are complex types or when scripting task execution. If both `--params` and individual options are provided, the JSON values win for any overlapping keys.

```bash
# error=true from --params overrides the model default error=false
python -m myapp exec fast --params '{"error": true}'
```

#### `--run-id`

Pass a custom run ID to correlate the task run with external systems:

```bash
python -m myapp exec add --run-id my-custom-id --params '{"a": 1, "b": 2}'
```

#### `--log`

Enable structured logging output during execution:

```bash
python -m myapp exec add --log --params '{"a": 1, "b": 2}'
```

## API reference

## fluid.scheduler.cli.TaskManagerCLI

```python
TaskManagerCLI(task_manager_app, log_config=None, **kwargs)
```

Bases: `LazyGroup`

CLI for TaskManager

This class provides a CLI for a TaskManager Application.

It requires to install the `cli` extra dependencies.

| PARAMETER          | DESCRIPTION                                                                                                                                                      |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `task_manager_app` | Task manager application. This can be a FastAPI app, a callable that returns a FastAPI app, or a string import path to a FastAPI app. **TYPE:** `TaskManagerApp` |
| `log_config`       | Log configuration parameters. These parameters are passed to the log.config function when configuring logging. **TYPE:** \`dict                                  |

Source code in `fluid/scheduler/cli.py`

```python
def __init__(
    self,
    task_manager_app: Annotated[
        TaskManagerApp,
        Doc("""
            Task manager application.

            This can be a FastAPI app, a callable that returns a FastAPI app,
            or a string import path to a FastAPI app.
            """),
    ],
    log_config: Annotated[
        dict | None,
        Doc("""
            Log configuration parameters.

            These parameters are passed to the [log.config][fluid.utils.log.config]
            function when configuring logging.
            """),
    ] = None,
    **kwargs: Any,
):
    kwargs.setdefault("commands", DEFAULT_COMMANDS)
    super().__init__(**kwargs)
    self.task_manager_app = task_manager_app
    self.log_config = log_config or {}
```

### task_manager_app

```python
task_manager_app = task_manager_app
```

### log_config

```python
log_config = log_config or {}
```

### lazy_subcommands

```python
lazy_subcommands = lazy_subcommands or {}
```

### get_task_manager_app

```python
get_task_manager_app()
```

Get the FastAPI app for the TaskManager.

Source code in `fluid/scheduler/cli.py`

```python
def get_task_manager_app(self) -> FastAPI:
    """Get the FastAPI app for the TaskManager."""
    if isinstance(self.task_manager_app, str):
        return import_from_string(self.task_manager_app)()
    elif isinstance(self.task_manager_app, FastAPI):
        return self.task_manager_app
    else:
        return self.task_manager_app()
```

### get_task_manager

```python
get_task_manager()
```

Get the TaskManager instance from the app state.

Source code in `fluid/scheduler/cli.py`

```python
def get_task_manager(self) -> TaskManager:
    """Get the TaskManager instance from the app state."""
    app = self.get_task_manager_app()
    if not hasattr(app.state, "task_manager"):
        raise RuntimeError("The app does not have a task_manager in its state")
    return app.state.task_manager
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
