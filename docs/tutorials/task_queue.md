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

They normally run on a subprocess and they can be defined by setting the `cpu_bound` flag to `True` in the [task][fluid.scheduler.task] decorator. They can perform heavy CPU bound operations without blocking the event loop.

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

#### How it works

When a CPU bound task is dispatched, the consumer spawns a **fresh Python subprocess** that imports the task's module and executes the task function in isolation. This keeps the consumer's asyncio event loop completely unblocked while the subprocess runs.

The subprocess is identified by the `TASK_MANAGER_SPAWN=true` environment variable. Inside the subprocess, `@task(cpu_bound=True)` behaves like a plain `@task` — the wrapper is transparent and the executor function runs directly without any extra subprocess indirection.

You can check whether your code is running inside a CPU subprocess:

```python
from fluid.scheduler.common import is_in_cpu_process

if is_in_cpu_process():
    # running inside the spawned subprocess
    ...
```

Stdout and stderr from the subprocess are streamed back to the consumer in real time, so logs produced by the task appear in the consumer's output.

#### Kubernetes

When the consumer is running inside a Kubernetes cluster, CPU bound tasks can be dispatched as Kubernetes Jobs instead of local subprocesses. See [K8s Jobs](task_k8s.md) for more details.


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

## Timeout

All tasks, both IO and CPU bound, respect the `timeout_seconds` parameter (default **60 seconds**). The timeout is measured from when the task starts executing.

For IO bound tasks, `asyncio` raises a `TimeoutError` if the coroutine has not completed within the timeout, and the task run transitions to the `failure` state. For CPU bound tasks, the subprocess (or Kubernetes Job) is killed and the run likewise transitions to `failure`.

```python
from fluid.scheduler import task, TaskRun

@task(timeout_seconds=300)
async def slow_io_task(ctx: TaskRun) -> None:
    ...

@task(cpu_bound=True, timeout_seconds=300)
async def slow_cpu_task(ctx: TaskRun) -> None:
    ...
```

For long-running tasks make sure to raise `timeout_seconds` to an appropriate value.

## Concurrency control

Use `max_concurrency` to limit how many instances of a task can run simultaneously. This applies to both IO and CPU bound tasks, and is useful to avoid overwhelming downstream services or exhausting system resources when many tasks are queued at once.

```python
from fluid.scheduler import task, TaskRun

@task(max_concurrency=5)
async def fetch_data(ctx: TaskRun) -> None:
    ...

@task(cpu_bound=True, max_concurrency=2)
async def heavy_calculation(ctx: TaskRun) -> None:
    ...
```

A value of `0` (the default) means no limit.

When the limit is reached the task run transitions to the `rate_limited` state. To automatically retry rate-limited tasks, combine `max_concurrency` with `rate_limit_retry`. See [Task Retry](task_retry.md) for details.

## Aborting a task

Any task — IO or CPU bound — can signal a deliberate, non-error cancellation by calling [ctx.abort()][fluid.scheduler.TaskRun.abort]:

```python
from fluid.scheduler import task, TaskRun

@task
async def conditional_work(ctx: TaskRun) -> None:
    if not should_proceed(ctx.params):
        ctx.abort("precondition not met")
    ...
```

When this happens the task run transitions to the `aborted` [TaskState][fluid.scheduler.TaskState], which is distinct from `failure`:

- the event is logged at **info** level, not as an error
- no retry policy is triggered
- any registered abort handlers (e.g. the database plugin) are still notified

### CPU-bound tasks

For CPU-bound tasks (subprocess or Kubernetes Job) the task function runs in a **separate process**, so the abort signal must be relayed back to the consumer. The mechanism works as follows:

1. The inner process calls `ctx.abort()`, which raises [TaskAbortedError][fluid.scheduler.errors.TaskAbortedError].
2. The consumer running *inside* that process catches the error and writes the reason to a short-lived Redis key (60-second TTL).
3. After the subprocess or k8s Job exits, the outer consumer reads the Redis key. If an abort reason is found it re-raises [TaskAbortedError][fluid.scheduler.errors.TaskAbortedError], marking the run as `aborted` instead of `success`.

This means a CPU-bound task that aborts itself is always correctly reflected as `aborted` in the task run state, regardless of whether it ran locally or as a Kubernetes Job.
