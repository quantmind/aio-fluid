# Tasks

Tasks are standard python async functions decorated with the [@task][fluid.scheduler.task] decorator.

```python
from fluid.scheduler import task, TaskRun

@task
async def say_hi(ctx: TaskRun) -> None:
    print("Hi!")
```

The [TaskRun][fluid.scheduler.TaskRun] object is passed to the task function and contains the task metadata, including optional parameters, and the [TaskManager][fluid.scheduler.TaskManager].

## Task Parameters

It is possible to pass parameters to the task, to do so, create a pydantic model
for the task parameters

```python
from pydantic import BaseModel

class TaskParams(BaseModel):
    name: str
```

and pass it to the `task` decorator

```python
from fluid.scheduler import task, TaskRun

@task
async def say_hi(ctx: TaskRun[TaskParams]) -> None:
    print(f"Hi {ctx.params.name}!")
```

## Task Types

There are few types of tasks implemented, lets take a look at them.

### IO Bound Tasks

They run concurrently with the [TaskConsumer][fluid.scheduler.TaskConsumer]. They must perform non blocking IO operations (no heavy CPU bound operations that blocks the event loop).

```python
from fluid.scheduler import task, TaskRun
from pydantic import BaseModel


class Scrape(BaseModel):
    url: str = "https://"


@task
async def fecth_data(ctx: TaskRun[Scrape]) -> None:
    # fetch data
    data = await http_cli.get(ctx.params.url)
    data_id = await datastore_cli.stote(data)
    # trigger another task
    ctx.task_manager.queue("heavy_calculation", data_id=data_id)
```

### CPU bound tasks

They run on a subprocess

```python
from fluid.scheduler import task, TaskRun

@task(cpu_bound=True)
async def heavy_calculation(ctx: TaskRun) -> None:
    data = await datastore_cli.get(ctx.params["data_id"])
    # perform some heavy calculation
    ...
    # trigger another task
    ctx.task_manager.queue("fetch_data")
```

### Scheduled Tasks

Both IO and CPU bound tasks can be periodically scheduled via the `schedule` keyword argument.

There are two types of scheduling, the most common is the [every][fluid.scheduler.every] function that takes a `timedelta` object.

```python
import asyncio
from datetime import timedelta
from fluid.scheduler import task, TaskRun, every

@task(schedule=every(timedelta(seconds=1)))
async def scheduled(ctx: TaskRun) -> None:
    await asyncio.sleep(0.1)
```

You can also use the [crontab][fluid.scheduler.crontab] function to schedule tasks using cron expressions.

```python
import asyncio
from fluid.scheduler import task, TaskRun, crontab

@task(schedule=crontab(hours='*/2'))
async def scheduled(ctx: TaskRun) -> None:
    await asyncio.sleep(0.1)
```
