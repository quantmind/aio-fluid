# Tasks

Tasks are standard python async functions decorated with the @task decorator.

```python
from fluid.scheduler import task, TaskRun

@task
async def say_hi(ctx: TaskRun) -> None:
    print("Hi!")
```

The TaskRun object is passed to the task function and contains the task metadata, including optional parameters, and the TaskManager.

## Task Parameters

It is possible to pass parameters to the task, to do so, create a pydantic model for the task parameters

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

They run concurrently with the TaskConsumer. They must perform non blocking IO operations (no heavy CPU bound operations that blocks the event loop).

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

They normally run on a subprocess and they can be defined by setting the `cpu_bound` flag to `True` in the task decorator. They can perform heavy CPU bound operations without blocking the event loop.

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

When a CPU bound task is dispatched, the consumer spawns a **fresh Python subprocess** which runs the task through the `exec` command of the application command line client. This keeps the consumer's asyncio event loop completely unblocked while the subprocess runs.

The command is derived from the one which started the consumer: the `serve` command is dropped, along with any option that belongs to it, and `exec <task-name>` is appended, with the run id and the task params passed as options. A [Kubernetes Job](https://fluid.quantmind.com/tutorials/task_k8s/index.md) derives its command from the consumer deployment in exactly the same way, so a task runs the same locally and in a cluster. It also means the entry point has to be a TaskManagerCLI: a consumer started any other way raises CpuBoundEntryPointError on startup.

The subprocess is identified by the `TASK_MANAGER_SPAWN=true` environment variable. Inside it, `@task(cpu_bound=True)` behaves like a plain `@task`, so the executor function runs directly without any extra subprocess indirection.

You can check whether your code is running inside a CPU subprocess:

```python
from fluid.scheduler.common import is_in_cpu_process

if is_in_cpu_process():
    # running inside the spawned subprocess
    ...
```

Stdout and stderr from the subprocess are streamed back to the consumer in real time, so logs produced by the task appear in the consumer's output.

A CPU bound task does not run in the consumer process, so it does not share the TaskManager instance the consumer is using. The process that executes the task builds its own task manager first, which is why an application with CPU bound tasks has to be set up with a command line entry point. See [Setup for CPU bound tasks](https://fluid.quantmind.com/tutorials/task_app/#setup-for-cpu-bound-tasks).

#### Kubernetes

When the consumer is running inside a Kubernetes cluster, CPU bound tasks can be dispatched as Kubernetes Jobs instead of local subprocesses. See [K8s Jobs](https://fluid.quantmind.com/tutorials/task_k8s/index.md) for more details.

### Scheduled Tasks

Both IO and CPU bound tasks can be periodically scheduled via the `schedule` keyword argument.

There are two types of scheduling, the most common is the every function that takes a `timedelta` object.

```python
import asyncio
from datetime import timedelta
from fluid.scheduler import task, TaskRun, every

@task(schedule=every(timedelta(seconds=1)))
async def scheduled(ctx: TaskRun) -> None:
    await asyncio.sleep(0.1)
```

You can also use the crontab function to schedule tasks using cron expressions.

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

When the limit is reached the task run transitions to the `rate_limited` state. To automatically retry rate-limited tasks, combine `max_concurrency` with `rate_limit_retry`. See [Task Retry](https://fluid.quantmind.com/tutorials/task_retry/index.md) for details.

## Chaining tasks

A task can queue another task with TaskRun.queue, which is how multi-step pipelines are built. Each step ends by queueing the next one and returns:

```python
from pydantic import BaseModel

from fluid.scheduler import TaskRun, TaskScheduler, task

SYMBOLS = ("BTC-USD", "ETH-USD")


class Symbol(BaseModel):
    symbol: str = "BTC-USD"


@task
async def daily_pipeline(ctx: TaskRun) -> None:
    """Start one chain per symbol."""
    for symbol in SYMBOLS:
        await ctx.queue(extract, symbol=symbol)


@task
async def extract(ctx: TaskRun[Symbol]) -> None:
    """Download the raw data, then hand over to the next step."""
    ctx.logger.info("extracting %s", ctx.params.symbol)
    # queue the next step and return, nothing blocks here.
    # if this task fails, transform is never queued
    await ctx.queue(transform, symbol=ctx.params.symbol)


@task
async def transform(ctx: TaskRun[Symbol]) -> None:
    """Normalise what extract downloaded."""
    ctx.logger.info(
        "transforming %s, chain started by %s", ctx.params.symbol, ctx.root_run_id
    )


def task_scheduler() -> TaskScheduler:
    scheduler = TaskScheduler()
    scheduler.register_from_dict(globals())
    return scheduler
```

Queueing rather than waiting is what you want here. The next step is a durable message in the broker, so a consumer restart does not lose the pipeline, any consumer in the fleet can pick the step up, and the parent does not sit idle occupying a run slot while the rest of the chain executes. Error handling falls out of the shape: a step that fails never reaches its `queue` call, so the chain stops there.

Every run queued this way records where it came from:

- from_run_id is the run that queued it.
- root_run_id is the first run in the chain, shared by every run descending from it, so a whole pipeline can be retrieved in one query.

Both are empty for a run that was not queued from another run, such as one started by a schedule or over HTTP.

### Waiting for a result

When the caller genuinely needs the outcome, for example an HTTP handler that must return it in the response, use TaskConsumer.queue_and_wait instead. Note that it waits in memory on the local task manager, so the wait (not the queued run) is lost if that process restarts, and the caller holds its run slot for the duration. Prefer chaining for anything that looks like a pipeline.

## Aborting a task

Any task, IO or CPU bound, can signal a deliberate, non-error cancellation by calling ctx.abort():

```python
from fluid.scheduler import task, TaskRun

@task
async def conditional_work(ctx: TaskRun) -> None:
    if not should_proceed(ctx.params):
        ctx.abort("precondition not met")
    ...
```

When this happens the task run transitions to the `aborted` TaskState, which is distinct from `failure`:

- the event is logged at **info** level, not as an error
- no retry policy is triggered
- any registered abort handlers (e.g. the database plugin) are still notified

### CPU-bound tasks

For CPU-bound tasks (subprocess or Kubernetes Job) the task function runs in a **separate process**, so the abort signal must be relayed back to the consumer. The mechanism works as follows:

1. The inner process calls `ctx.abort()`, which raises TaskAbortedError.
1. The consumer running *inside* that process catches the error and writes the reason to a short-lived Redis key (60-second TTL).
1. After the subprocess or k8s Job exits, the outer consumer reads the Redis key. If an abort reason is found it re-raises TaskAbortedError, marking the run as `aborted` instead of `success`.

This means a CPU-bound task that aborts itself is always correctly reflected as `aborted` in the task run state, regardless of whether it ran locally or as a Kubernetes Job.
