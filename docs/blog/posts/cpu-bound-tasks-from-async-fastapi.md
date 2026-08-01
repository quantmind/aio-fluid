---
date: 2026-08-01
---

# Running CPU-bound tasks from an async FastAPI app without freezing your event loop

If you run an async Python service — FastAPI, aiohttp, a bare asyncio app — you have almost
certainly hit this wall:

Everything is fast and concurrent right up until one request needs to do *real* CPU work. Parse a
big file. Run a pandas aggregation. Render a PDF. Score a model. The moment that work starts, your
single event loop stops turning. Health checks time out. Every other in-flight request hangs. One
CPU-heavy task took your whole process hostage.

<!-- more -->

`asyncio` is cooperative. A coroutine that spends two seconds in NumPy is two seconds where *nothing
else runs* — no other requests, no heartbeats, no graceful shutdown. `await` doesn't help; there's
nothing to await. This is the defining limitation of async Python, and it's why "just make it async"
is not an answer for CPU-bound work.

## The usual answers, and why they chafe

- **`run_in_executor` / a thread pool.** Threads don't escape the GIL for pure-Python or
  many C-extension paths, so you often get concurrency on paper and a serialized bottleneck in
  practice. And a runaway thread is hard to cancel.
- **`ProcessPoolExecutor`.** Now you're pickling arguments, managing a pool lifecycle, and losing
  your task's structured context. It works, but it's plumbing you write and re-write per project.
- **Celery.** Battle-tested and powerful — but it predates asyncio, and the standard advice is to
  run a *separate* worker fleet for CPU work. Two deployments, two scaling stories, two things to
  operate.
- **Async-native queues (`arq`, `taskiq`).** Lovely for IO-bound tasks, but they assume your tasks
  never block the loop. CPU-bound work is out of scope by design.

Every one of these makes the same implicit demand: *decide up front whether a task is IO-bound or
CPU-bound, and wire it into a different execution path.* Change your mind later and you rewrite.

## What if it were one flag?

`aio-fluid` is an async task queue where CPU-bound work is a first-class task type. You write a
normal `async def` task. If it's CPU-heavy, you add `cpu_bound=True`:

```python
from fluid.scheduler import task, TaskRun
from pydantic import BaseModel


class Report(BaseModel):
    rows: int = 1_000_000


# IO-bound: runs concurrently on the event loop, like any async task queue.
@task
async def fetch(ctx: TaskRun) -> None:
    ctx.task_manager.queue("crunch", rows=5_000_000)


# CPU-bound: same decorator, one flag. Runs in a fresh subprocess, so this
# blocking pandas call never touches your event loop.
@task(cpu_bound=True, timeout_seconds=600)
async def crunch(ctx: TaskRun[Report]) -> None:
    heavy_pandas_work(ctx.params.rows)  # blocking is fine — it's isolated
```

When a `cpu_bound` task is dispatched, the consumer spawns a **fresh Python subprocess**, imports the
task's module, and runs the function there. The subprocess has its own interpreter and its own GIL,
so it genuinely runs in parallel with your event loop — which stays free to serve requests, answer
health checks, and shut down cleanly. Stdout and stderr stream back to the consumer in real time, so
the task's logs show up where you'd expect. Task parameters are [pydantic](https://docs.pydantic.dev/)
models, validated on the way in, so the subprocess boundary stays typed instead of becoming a bag of
pickled positional args.

## The part that made me write this post

Here's the bit I actually think is neat. That *same task, unchanged*, behaves differently depending
on where it runs.

Run your consumer inside a Kubernetes cluster (with the `k8s` extra installed), and every
`cpu_bound=True` task dispatches as a **Kubernetes Job** instead of a local subprocess. No code
change. No second decorator. No separate worker deployment to define and maintain. The switch is
automatic — `aio-fluid` keys off the `KUBERNETES_SERVICE_HOST` variable that Kubernetes injects into
every pod.

The Job's pod template is *derived from your consumer's own deployment*: same image, same volume
mounts, same security context, same env — it reads the deployment and builds the Job spec from it,
overriding only what's needed to run the one task and clearing what doesn't apply (liveness and
readiness probes, sidecars). Jobs clean themselves up via `ttlSecondsAfterFinished`. A failed Job
propagates the error back to the consumer instead of silently retrying.

So the mental model is: **write the task once; it scales down to a subprocess on your laptop and out
to a dedicated pod in production, with the same three lines of Python.** Your web service and your
heavy compute share one codebase and one deployment definition, and you never provisioned a Celery
worker fleet to get there. The full mechanics are in the [K8s Jobs tutorial](../../tutorials/task_k8s.md).

## Wiring it into FastAPI

The task manager drops straight into a FastAPI app, so you can enqueue work from a request handler
and inspect runs over HTTP:

```python
from fastapi import FastAPI
from fluid.scheduler import TaskScheduler, task_manager_fastapi

scheduler = TaskScheduler()
scheduler.register_from_dict(globals())  # registers the @task functions above

app = task_manager_fastapi(scheduler, app=FastAPI(title="My Service"))
```

A `POST` to enqueue `crunch` returns immediately; the heavy work runs in a subprocess (or a k8s Job)
while your API keeps serving. That's the whole point: **the request path never blocks on CPU.**

## When *not* to reach for this

I'd rather be honest than oversell:

- If you have zero CPU-bound work, a pure async queue like `arq` or `taskiq` is lighter and there's
  no reason to switch.
- If you're deep in the Celery ecosystem and happy with a separate worker fleet, that maturity and
  breadth is real — `aio-fluid` is younger. See how the landscape stacks up in the
  [task-queue comparison](https://fluid.quantmind.com/comparison/).
- `aio-fluid` uses Redis as its default broker. If you've standardized on RabbitMQ or SQS, check the
  broker interface fits before committing.

But if you run an async service and have ever watched one heavy task freeze the whole thing — or
you're dreading standing up a parallel worker deployment just to move CPU work off the loop — this is
built for exactly that.

## Try it

```bash
pip install aio-fluid           # core task queue
pip install aio-fluid[cli,k8s]  # + Kubernetes Job offload
```

- [Tasks tutorial](../../tutorials/task_queue.md)
- [CPU bound tasks](../../tutorials/task_queue.md#cpu-bound-tasks)
- [Kubernetes Jobs](../../tutorials/task_k8s.md)

If you try it, I'd genuinely like to hear where it breaks — [open an issue](https://github.com/quantmind/aio-fluid/issues).
