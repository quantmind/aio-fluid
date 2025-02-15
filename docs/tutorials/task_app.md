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

The `TaskConsumer` can be integrated with FastAPI so that
tasks can be queued via HTTP requests.

```python
import uvicorn
from fluid.scheduler.endpoints import setup_fastapi

if __name__ == "__main__":
    consumer = task_consumer()
    app = setup_fastapi(consumer)
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

if __name__ == "__main__":
    consumer = task_consumer()
    TaskManagerCLI(setup_fastapi(consumer))()
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
  exec   Execute a registered task
  ls     List all tasks with their schedules
  serve  Start app server
```

The command line tool provides a powerful interface to execute tasks, parameters are
passed as optional arguments using the standard click interface.
