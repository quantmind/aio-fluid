# Task Queue App

The `fluid.scheduler` module is a simple yet powerful distributed task producer ([TaskScheduler][fluid.scheduler.TaskScheduler]) and consumer ([TaskConsumer][fluid.scheduler.TaskConsumer]) system for executing tasks.
The middleware for distributing tasks can be configured via the [TaskBroker][fluid.scheduler.TaskBroker] interface.

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

## FastAPI Integration

A [TaskManager][fluid.scheduler.TaskManager] can be integrated with FastAPI so that
tasks can be queued via HTTP requests.

To setup the FastAPI app, use the [task_manager_fastapi][fluid.scheduler.task_manager_fastapi] function:

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

and check the openapi UI at [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).


## Task App Command Line

The [TaskConsumer][fluid.scheduler.TaskConsumer] or [TaskScheduler][fluid.scheduler.TaskScheduler] can be run with the command line tool to allow for an even richer API.

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

The command line tool provides a powerful interface to execute tasks, parameters are
passed as optional arguments using the standard click interface.

## Plugins

Plugins extend the task manager with additional behaviour by hooking into task lifecycle
events. A plugin implements the [TaskManagerPlugin][fluid.scheduler.TaskManagerPlugin]
interface and is registered via [TaskManager.with_plugin][fluid.scheduler.TaskManager.with_plugin].

### Database Plugin

The [TaskDbPlugin][fluid.scheduler.db.TaskDbPlugin] stores every task run in a
database table so you can query task history, audit outcomes, and build dashboards
on top of the data.

It requires a [CrudDB][fluid.db.CrudDB] instance and the `db` extra:

```bash
pip install aio-fluid[db]
```

Register the plugin when building your task manager:

```python
from fluid.scheduler import TaskScheduler
from fluid.scheduler.db import TaskDbPlugin
from fluid.db import CrudDB

db = CrudDB.from_env()
task_manager = TaskScheduler(...)
task_manager.with_plugin(TaskDbPlugin(db))
```

The plugin creates a `fluid_tasks` table (configurable via `table_name`) and
persists a row for each task run as it moves through its lifecycle states.
Tasks tagged with `skip_db` are excluded from persistence.

### Custom Plugins

To create your own plugin, subclass [TaskManagerPlugin][fluid.scheduler.TaskManagerPlugin]
and implement the `register` method. Use
[TaskManager.register_async_handler][fluid.scheduler.TaskManager.register_async_handler]
to subscribe to task lifecycle events:

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
