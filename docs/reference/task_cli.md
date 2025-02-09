# Task Manager Cli

Command line tools for task manager applications.

This modules requires the `cli` extra to be installed.

```bash
$ pip install aio-fluid[cli]
```
It can be imported from `fluid.scheduler.cli`:

```python
from fastapi.scheduler.cli import TaskManagerCLI

if __name__ == "__main__":
    cli = TaskManagerCLI("path.to:task_app")
    cli()
```

::: fluid.scheduler.cli.TaskManagerCLI
