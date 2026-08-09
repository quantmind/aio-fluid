# Task Queue App

The `fluid.scheduler` module is a simple yet powerful distributed task producer (TaskScheduler) and consumer (TaskConsumer) system for executing tasks. The middleware for distributing tasks can be configured via the TaskBroker interface.

A redis task broker is provided for convenience.

## Tasks Consumer

Create a task consumer, register tasks from modules, and run the consumer.

```python
import asyncio
from typing import Any
from fluid.scheduler import TaskConsumer
import task_module_a, task_module_b


def task_consumer(**kwargs: Any) -> TaskConsumer:
    consumer = TaskConsumer(**kwargs)
    consumer.register_from_module(task_module_a)
    consumer.register_from_module(task_module_b)
    return consumer


if __name__ == "__main__":
    consumer = task_consumer()
    asyncio.run(consumer.run())
```

Pass `tags` to register_from_module (or register_from_dict / register_task) to add extra tags to every registered task on top of the tags already declared on each task:

```python
consumer.register_from_module(task_module_a, tags=["module-a"])
```

## FastAPI Integration

A TaskManager can be integrated with FastAPI so that tasks can be queued via HTTP requests.

To setup the FastAPI app, use the task_manager_fastapi function:

```python
import uvicorn
from fluid.scheduler import task_manager_fastapi

if __name__ == "__main__":
    consumer = task_consumer()
    app = task_manager_fastapi(consumer)
    uvicorn.run(app)
```

You can test via the example provided

```bash
$ python -m examples.simple_fastapi
```

and check the openapi UI at <http://127.0.0.1:8000/docs>.

The app returned is an ordinary FastAPI app: your own routes can be added to it and reach the task manager, its dependencies and its resources. See [Extending the FastAPI App](https://fluid.quantmind.com/tutorials/task_fastapi/index.md).

The `GET /tasks` endpoint lists registered tasks and accepts a repeatable `tags` query parameter to only return tasks that have at least one of the given tags:

```text
GET /tasks
GET /tasks?tags=fast&tags=slow
```

## Task App Command Line

The TaskConsumer or TaskScheduler can be run with the command line tool to allow for an even richer API.

```python
from fluid.scheduler.cli import TaskManagerCLI
from fluid.scheduler import task_manager_fastapi

if __name__ == "__main__":
    consumer = task_consumer()
    TaskManagerCLI(task_manager_fastapi(consumer))()
```

This features requires to install the package with the `cli` extra.

```bash
$ pip install aio-fluid[cli]
```

```bash
$ python -m examples.simple_cli
Usage: python -m examples.simple_cli [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  enable  Enable or disable a task
  exec    Execute a registered task
  ls      List all tasks with their schedules
  serve   Start app server.
```

The command line tool provides a powerful interface to execute tasks, parameters are passed as optional arguments using the standard click interface.

## Setup for CPU bound tasks

For an application with [CPU bound tasks](https://fluid.quantmind.com/tutorials/tasks/#cpu-bound-tasks) the command line entry point above is not optional, it is how those tasks are executed.

A CPU bound task does not run in the consumer process, so it does not share the TaskManager instance the consumer is using. The process that runs the task builds its own task manager first, through the `exec` command, which means everything the application attaches to the manager, dependencies and plugins in particular, has to be built again there.

So build the task manager in the entry point, with the same dependencies and plugins the consumer uses, and expose it through TaskManagerCLI. A task then behaves the same whether it runs on the event loop or in a separate process, and the `cli` extra becomes a requirement rather than an option.

The same entry point is what a [Kubernetes Job](https://fluid.quantmind.com/tutorials/task_k8s/index.md) runs when a CPU bound task is dispatched in a cluster, with the Job command derived from the consumer deployment.

## Plugins

Plugins extend the task manager with additional behaviour by hooking into task lifecycle events. A plugin implements the TaskManagerPlugin interface and is registered via TaskManager.with_plugin.

### Database Plugin

The TaskDbPlugin stores every task run in a database table so you can query task history, audit outcomes, and build dashboards on top of the data.

It requires a CrudDB instance and the `db` extra:

```bash
pip install aio-fluid[db]
```

Register the plugin when building your task manager:

```python
from fluid.scheduler import TaskScheduler, task_manager_fastapi
from fluid.scheduler.db import TaskDbPlugin
from fluid.db import CrudDB

db = CrudDB.from_env()
task_manager = TaskScheduler(...)
task_manager.with_plugin(TaskDbPlugin(db))
app = task_manager_fastapi(task_manager)
```

The plugin creates a `fluid_tasks` table (configurable via `table_name`) and persists a row for each task run as it moves through its lifecycle states. Tasks tagged with `skip_db` are excluded from persistence. The plugin mounts a `/tasks-history` router on the app with two endpoints:

| Method | Path                      | Description                                 |
| ------ | ------------------------- | ------------------------------------------- |
| `GET`  | `/tasks-history`          | List task run history with optional filters |
| `GET`  | `/tasks-history/{run_id}` | Fetch a single task run by ID               |

The list endpoint accepts the following query parameters:

| Parameter | Type        | Description                                      |
| --------- | ----------- | ------------------------------------------------ |
| `name`    | `string`    | Filter by task name                              |
| `state`   | `TaskState` | Filter by task state (e.g. `success`, `failure`) |
| `start`   | `datetime`  | Only runs queued at or after this time           |
| `end`     | `datetime`  | Only runs queued at or before this time          |

Example requests:

```bash
# All history, most recent first
GET /history

# Only successful runs of the "add" task
GET /history?name=add&state=success

# Runs queued in a specific time window
GET /history?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z

# Fetch a specific run by ID
GET /history/abc123
```

### Custom Plugins

To create your own plugin, subclass TaskManagerPlugin and implement the `register` method. Use TaskManager.register_async_handler to subscribe to task lifecycle events:

```python
from fluid.scheduler import TaskManagerPlugin, TaskManager, TaskState
from fluid.utils.dispatcher import Event


class MyPlugin(TaskManagerPlugin):
    def register(self, task_manager: TaskManager) -> None:
        task_manager.register_async_handler(
            Event(TaskState.success, "my_plugin"),
            self._on_success,
        )

    async def _on_success(self, task_run) -> None:
        print(f"Task {task_run.name} succeeded")
```
