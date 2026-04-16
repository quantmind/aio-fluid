# Task Manager CLI

Command line tools for [TaskManager][fluid.scheduler.TaskManager] applications.

This module requires the `cli` extra to be installed:

```bash
pip install aio-fluid[cli]
```

## Setup

Wrap your FastAPI app with [TaskManagerCLI][fluid.scheduler.cli.TaskManagerCLI] and call it as the entry point:

```python
from fluid.scheduler.cli import TaskManagerCLI

task_manager_cli = TaskManagerCLI("examples.tasks:task_app")

if __name__ == "__main__":
    task_manager_cli()
```

`task_manager_app` can be a FastAPI instance, a callable that returns one, or a dotted-import string (resolved at call time so the CLI stays importable without loading the full app).

## Commands

### `ls` — list registered tasks

Prints a table of all registered tasks with their schedule, priority, timeout, and CPU-bound flag.

```bash
python -m myapp ls
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

Each task exposes its [params model][fluid.scheduler.task] fields as CLI options (via
[pydanclick](https://github.com/felix-martel/pydanclick)):

```bash
python -m myapp exec add --a 5 --b 3
```

#### Passing parameters as JSON

All task parameters can also be supplied together as a JSON string with `--params`.
Values in `--params` **always take priority** over individual CLI option defaults:

```bash
python -m myapp exec add --params '{"a": 5, "b": 3}'
```

This is useful when parameters are complex types or when scripting task execution.
If both `--params` and individual options are provided, the JSON values win for any
overlapping keys.

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

::: fluid.scheduler.cli.TaskManagerCLI
