# Task Managers

Three classes can hold your tasks, and they form a chain where each one adds a single capability to the one before it:

```text
TaskManager  ->  TaskConsumer  ->  TaskScheduler
   queue          + execute          + schedule
```

TaskConsumer is a TaskManager that also runs tasks, and TaskScheduler is a TaskConsumer that also fires schedules. Pick the smallest one that covers what the process has to do.

|                                                     | TaskManager | TaskConsumer | TaskScheduler |
| --------------------------------------------------- | ----------- | ------------ | ------------- |
| Register tasks                                      | ✅          | ✅           | ✅            |
| Queue a task run on the broker                      | ✅          | ✅           | ✅            |
| Execute a task inline, bypassing the queue          | ✅          | ✅           | ✅            |
| Is a Workers, with a start/stop lifecycle           | ❌          | ✅           | ✅            |
| Consumes the queue and runs what it finds           | ❌          | ✅           | ✅            |
| Fires `schedule=` tasks when they are due           | ❌          | ❌           | ✅            |
| Async event handlers, and the plugins built on them | ❌          | ✅           | ✅            |

## TaskManager

The base class owns the pieces every application needs: the registry, the broker connection, the `deps` object and the plugin list. It can put work on the queue with TaskManager.queue, and it can run a task there and then with TaskManager.execute, which skips the queue and awaits the task on the current event loop.

What it does not do is run anything in the background. It is not a worker: no coroutine of its own is ever started, so a task queued by a TaskManager sits on the broker until some consumer elsewhere picks it up.

One consequence is easy to miss. TaskManager.register_async_handler is a no-op on the base class, because the worker that dispatches those events only exists on a consumer. Plugins built on lifecycle events, the database plugin among them, therefore record nothing when attached to a plain TaskManager. Attach them to the process that consumes the queue.

Use it when the process submits work but must never spend its own capacity running it: an HTTP API in front of a separate worker fleet, a cron container that queues one run and exits, a test that drives a task directly.

## TaskConsumer

TaskConsumer adds the machinery that executes queued work. It starts `max_concurrent_tasks` coroutine workers, each pulling one task run at a time from the broker, so that setting is the concurrency ceiling for the process. Around them it starts the worker that dispatches async events, the in-process queue that holds delayed runs until they are due, and a heartbeat that publishes the manager status other processes read through `GET /tasks-status`.

Being a worker, it starts and stops with whatever runs it, and TaskConsumer.queue_and_wait becomes available: queue a run and await its result.

A consumer does not look at schedules. Registering a task declared with `schedule=every(...)` on a TaskConsumer gives you a task that is ready to run and never triggers, because nothing in the process is watching the clock. Something has to queue it, and that something is a scheduler.

Use it for worker deployments you scale horizontally: every replica consumes the same queue, and running ten of them multiplies throughput without any of them duplicating work.

## TaskScheduler

TaskScheduler adds one more worker, which ticks on a short heartbeat, asks the broker for the enabled tasks that have a schedule, and evaluates each every or crontab rule against the current time. A task that is due is queued on the broker like any other run.

That last detail is what makes the design scale: the scheduler publishes to the shared queue, it does not execute the run itself. Any consumer on the same broker may pick it up, so the process that owns the clock is not the bottleneck. A scheduler consumes the queue as well, since it is a consumer, and a small deployment can be a single TaskScheduler doing both jobs.

Run **one** scheduler per broker. Each scheduler keeps its own record of what it last fired, in memory, with no coordination between processes, so two schedulers on one broker queue every due task twice. Consumers are the part you replicate, not the scheduler.

The two flags on TaskManagerConfig let you split the roles without changing class:

```python
TaskScheduler(schedule_tasks=False)  # behaves as a consumer
TaskScheduler(consume_tasks=False)   # schedules only, runs nothing
```

## In a FastAPI app

task_manager_fastapi accepts any of the three, and the choice decides what the app process does, because the task routes themselves are identical in every case. `POST /tasks/{name}` queues a run whichever manager is behind it.

When the manager is a worker, a consumer or a scheduler, it is added to the app workers and starts and stops with the app: serving requests and running tasks happen in the same process. When it is a plain TaskManager, the app only gets startup and shutdown hooks, enough to open and close the resources registered with add_async_context_manager, and nothing consumes the queue.

So the same tasks module can be served by two entry points, an API that only produces and a worker that schedules and executes:

```python
from fastapi import FastAPI

from examples.docs import task_deps
from fluid.scheduler import TaskManager, TaskScheduler, task_manager_fastapi


def api_app() -> FastAPI:
    """Frontend app, it queues task runs but never executes them.

    It registers the tasks because a task must be in the registry to be queued,
    but it needs no dependencies: nothing runs here.
    """
    task_manager = TaskManager()
    task_manager.register_from_module(task_deps)
    return task_manager_fastapi(task_manager, title="Task producer")


def worker_app() -> FastAPI:
    """Worker app, it schedules and executes the same tasks."""
    deps = task_deps.Deps()
    scheduler = TaskScheduler(deps=deps)
    scheduler.add_async_context_manager(deps.http_client)
    scheduler.register_from_module(task_deps)
    return task_manager_fastapi(scheduler, title="Task worker")
```

Both entry points register the tasks, and they have to: a task must be in the registry to be queued by name, and a consumer that receives a run for a task it does not know logs an unknown task error. The dependencies, on the other hand, are only needed where tasks actually run, which is why the producer above builds none.

Deploy that pair as one API deployment scaled for traffic and one worker deployment scaled for load, sharing a broker. For a single service doing both, pass a TaskScheduler and be done. See [Extending the FastAPI App](https://fluid.quantmind.com/tutorials/task_fastapi/index.md) for reaching whichever manager you chose from your own routes.

## Choosing

- Only queueing work, or executing it inline in tests and scripts, use TaskManager.
- Running queued work, with no schedules in the process, use TaskConsumer.
- Owning the clock, and usually consuming too, use TaskScheduler. This is the default choice for a single-service application.

Whichever you pick, an application with [CPU bound tasks](https://fluid.quantmind.com/tutorials/tasks/#cpu-bound-tasks) needs TaskManagerCLI as its entry point, because the subprocess that runs such a task builds its own task manager from it. See [Setup for CPU bound tasks](https://fluid.quantmind.com/tutorials/task_app/#setup-for-cpu-bound-tasks).
