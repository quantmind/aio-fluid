<h1>
  <a href="https://fluid.quantmind.com/">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://fluid.quantmind.com/assets/fluid-banner-light.svg" style="max-width: 400px; width: 100%;">
      <img alt="aio-fluid — async utilities for backend Python services" src="https://fluid.quantmind.com/assets/fluid-banner-dark.svg" style="max-width: 400px; width: 100%;">
    </picture>
  </a>
</h1>

**An async task queue that offloads CPU-bound work to subprocesses or Kubernetes Jobs**

[![PyPI version](https://badge.fury.io/py/aio-fluid.svg)](https://badge.fury.io/py/aio-fluid)
[![Python versions](https://img.shields.io/pypi/pyversions/aio-fluid.svg)](https://pypi.org/project/aio-fluid)
[![Python downloads](https://static.pepy.tech/badge/aio-fluid/month)](https://pepy.tech/project/aio-fluid)
[![build](https://github.com/quantmind/fluid/workflows/build/badge.svg)](https://github.com/quantmind/aio-fluid/actions?query=workflow%3Abuild)
[![codecov](https://codecov.io/gh/quantmind/aio-fluid/graph/badge.svg?token=81oWUoyEVp)](https://codecov.io/gh/quantmind/aio-fluid)

**Documentation**: [fluid.quantmind.com](https://fluid.quantmind.com/)

**Source**: [github.com/quantmind/aio-fluid](https://github.com/quantmind/aio-fluid)

Declare tasks with a `@task` decorator, schedule them with `every()` or cron, and run them
concurrently on asyncio. The part that sets `aio-fluid` apart: mark a task `cpu_bound=True` and
it runs in a **fresh subprocess** so heavy CPU work never freezes the event loop. And when your
consumer runs inside Kubernetes, the *same task* dispatches as a **Kubernetes Job** instead, with
**no code change**.

```python
import os
from datetime import timedelta

from fastapi import FastAPI
from pydantic import BaseModel

from fluid.scheduler import TaskRun, TaskScheduler, every, task, task_manager_fastapi
from fluid.scheduler.cli import TaskManagerCLI


class Report(BaseModel):
    rows: int = 5_000_000


def heavy_pandas_work(rows: int) -> None:
    """Stand-in for the CPU-heavy work you would do in a real task."""
    sum(range(rows))


@task(schedule=every(timedelta(seconds=5)))
async def heartbeat(ctx: TaskRun) -> None:
    """IO-bound task, scheduled every five seconds

    runs concurrently on the event loop
    """
    ctx.logger.info("still alive")


@task(
    cpu_bound=True,
    schedule=every(timedelta(seconds=20), delay=timedelta(seconds=5)),
    timeout_seconds=600,
)
async def crunch(ctx: TaskRun[Report]) -> None:
    """CPU-bound task, scheduled every 20 seconds with an initial delay of 5 seconds

    Same decorator, one flag. Runs in a subprocess (or a Kubernetes Job in-cluster)
    so the heavy work never blocks the event loop.
    Identical code in both places.
    """
    heavy_pandas_work(ctx.params.rows)
    ctx.logger.info("crunch finished on pid %d", os.getpid())


def scheduler_app() -> FastAPI:
    scheduler = TaskScheduler()
    scheduler.register_from_dict(globals())
    return task_manager_fastapi(scheduler)


if __name__ == "__main__":
    TaskManagerCLI(
        scheduler_app,
        help="Simple Task Manager CLI with default commands",
        log_config=dict(app_names=("__main__", "fluid")),
    )()
```

## Why aio-fluid?

Most Python task queues force a choice: async-native runners (`arq`, `taskiq`) that assume your work
never blocks the loop, or heavyweight brokers (`Celery`) that predate asyncio. Neither has a clean
answer for *"this one task is CPU-heavy"* beyond "spin up a second worker fleet."

`aio-fluid` treats CPU-bound work as a first-class task type:

- **One decorator, two execution models.** `@task(cpu_bound=True)` runs locally as a subprocess and
  in-cluster as a Kubernetes Job: the switch is automatic (`KUBERNETES_SERVICE_HOST` + the `k8s`
  extra). Your task code is identical in both. See [K8s Jobs](https://fluid.quantmind.com/tutorials/task_k8s/).
- **Async-native and typed.** Tasks are plain `async def` functions; parameters are
  [pydantic](https://docs.pydantic.dev/) models, validated on the way in.
- **The scheduling you expect.** `every(timedelta(...))` and `crontab(...)`, per-task
  `max_concurrency`, priorities, `timeout_seconds`, and retry policies.
- **FastAPI-ready.** Drop a task manager into a FastAPI app to queue and inspect runs over HTTP.
- **Task lifecycle callbacks.** Every state a run moves through (`queued`, `running`, `success`,
  `failure`, `aborted`) is dispatched as an event you can subscribe to, with sync or async
  handlers, so metrics, alerting and bookkeeping hang off the queue instead of your task code.
- **Task manager plugins.** Plugins hook into those same events and can mount their own HTTP
  routes. The bundled database plugin persists every run to Postgres and serves a
  `/tasks-history` API on top of it. See [Plugins](https://fluid.quantmind.com/tutorials/task_app/#plugins).
- **Pluggable broker.** Redis by default; the broker is an interface, not a hard dependency.

`Celery` is the mature, battle-tested default with the biggest ecosystem; reach for it when you
need that breadth. `aio-fluid` is for async services that want CPU-bound work handled natively and
scaled onto Kubernetes without a parallel worker deployment. For a feature-by-feature look at
`aio-fluid` next to Celery, RQ, arq and taskiq, backed by download data, see
[Python task queues compared](https://fluid.quantmind.com/comparison/).

## Batteries included

Alongside the task queue, `aio-fluid` ships the building blocks Quantmind uses to run backend
services:

- **Async workers**: composable components with a managed start/stop lifecycle; the foundation the task queue is built on. See [Workers](https://fluid.quantmind.com/reference/workers/).
- **Async Postgres CRUD**: a typed CRUD layer over `asyncpg` and SQLAlchemy, with pagination and schema migrations. See [Database](https://fluid.quantmind.com/reference/db/).
- **Event dispatchers**: sync and async `Dispatcher` types for decoupling event sources from handlers. See [Dispatchers](https://fluid.quantmind.com/reference/dispatchers/).
- **HTTP client helpers**: a unified async client wrapping `httpx` and `aiohttp`. See [HTTP Client](https://fluid.quantmind.com/reference/http_client/).
- **CLI tooling**: ready-made `click` / `rich` command-line interfaces for task managers and databases.

## Installation

This is a python package you can install via pip:

```
pip install aio-fluid
```

To install all the dependencies:

```
pip install aio-fluid[cli, db, http, log, k8s]
```
this includes the following extra dependencies:

- `cli` for the command line interface using [click](https://click.palletsprojects.com/) and [rich](https://github.com/Textualize/rich)
- `db` for database support with [asyncpg](https://github.com/MagicStack/asyncpg) and [sqlalchemy](https://www.sqlalchemy.org/)
- `http` for http client support with [httpx](https://www.python-httpx.org/) and [aiohttp](https://docs.aiohttp.org/en/stable/)
- `log` for JSON logging support with [python-json-logger](https://github.com/madzak/python-json-logger)
- `k8s` for Kubernetes support for CPU bound tasks

## Development

You can run the examples via

```
uv run python -m examples
```

We use [uv](https://uv.run/) as a development tool to run the examples and tests, but you can also use python directly if that's your preference.

## License

This project is licensed under the BSD License - see the [LICENSE](https://github.com/quantmind/aio-fluid/blob/main/LICENSE) file for details.
