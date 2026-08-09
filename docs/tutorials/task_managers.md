# Task Managers

Three classes can hold your tasks, and they form a chain where each one adds a
single capability to the one before it:

```
TaskManager  ->  TaskConsumer  ->  TaskScheduler
   queue          + execute          + schedule
```

[TaskConsumer][fluid.scheduler.TaskConsumer] is a
[TaskManager][fluid.scheduler.TaskManager] that also runs tasks, and
[TaskScheduler][fluid.scheduler.TaskScheduler] is a
[TaskConsumer][fluid.scheduler.TaskConsumer] that also fires schedules. Pick the
smallest one that covers what the process has to do.

| | [TaskManager][fluid.scheduler.TaskManager] | [TaskConsumer][fluid.scheduler.TaskConsumer] | [TaskScheduler][fluid.scheduler.TaskScheduler] |
|---|:-:|:-:|:-:|
| Register tasks | ✅ | ✅ | ✅ |
| Queue a task run on the broker | ✅ | ✅ | ✅ |
| Execute a task inline, bypassing the queue | ✅ | ✅ | ✅ |
| Is a [Workers][fluid.utils.worker.Workers], with a start/stop lifecycle | ❌ | ✅ | ✅ |
| Consumes the queue and runs what it finds | ❌ | ✅ | ✅ |
| Fires `schedule=` tasks when they are due | ❌ | ❌ | ✅ |
| Async event handlers, and the plugins built on them | ❌ | ✅ | ✅ |

## TaskManager

The base class owns the pieces every application needs: the registry, the
broker connection, the `deps` object and the plugin list. It can put work on the
queue with [TaskManager.queue][fluid.scheduler.TaskManager.queue], and it can run
a task there and then with
[TaskManager.execute][fluid.scheduler.TaskManager.execute], which skips the queue
and awaits the task on the current event loop.

What it does not do is run anything in the background. It is not a worker: no
coroutine of its own is ever started, so a task queued by a
[TaskManager][fluid.scheduler.TaskManager] sits on the broker until some consumer
elsewhere picks it up.

One consequence is easy to miss.
[TaskManager.register_async_handler][fluid.scheduler.TaskManager.register_async_handler]
is a no-op on the base class, because the worker that dispatches those events
only exists on a consumer. Plugins built on lifecycle events, the database plugin
among them, therefore record nothing when attached to a plain
[TaskManager][fluid.scheduler.TaskManager]. Attach them to the process that
consumes the queue.

Use it when the process submits work but must never spend its own capacity
running it: an HTTP API in front of a separate worker fleet, a cron container
that queues one run and exits, a test that drives a task directly.

## TaskConsumer

[TaskConsumer][fluid.scheduler.TaskConsumer] adds the machinery that executes
queued work. It starts `max_concurrent_tasks` coroutine workers, each pulling one
task run at a time from the broker, so that setting is the concurrency ceiling
for the process. Around them it starts the worker that dispatches async events,
the in-process queue that holds delayed runs until they are due, and a heartbeat
that publishes the manager status other processes read through
`GET /tasks-status`.

Being a worker, it starts and stops with whatever runs it, and
[TaskConsumer.queue_and_wait][fluid.scheduler.TaskConsumer.queue_and_wait]
becomes available: queue a run and await its result.

A consumer does not look at schedules. Registering a task declared with
`schedule=every(...)` on a [TaskConsumer][fluid.scheduler.TaskConsumer] gives you
a task that is ready to run and never triggers, because nothing in the process
is watching the clock. Something has to queue it, and that something is a
scheduler.

Use it for worker deployments you scale horizontally: every replica consumes the
same queue, and running ten of them multiplies throughput without any of them
duplicating work.

## TaskScheduler

[TaskScheduler][fluid.scheduler.TaskScheduler] adds one more worker, which ticks
on a short heartbeat, asks the broker for the enabled tasks that have a schedule,
and evaluates each [every][fluid.scheduler.every] or
[crontab][fluid.scheduler.crontab] rule against the current time. A task that is
due is queued on the broker like any other run.

That last detail is what makes the design scale: the scheduler publishes to the
shared queue, it does not execute the run itself. Any consumer on the same broker
may pick it up, so the process that owns the clock is not the bottleneck. A
scheduler consumes the queue as well, since it is a consumer, and a small
deployment can be a single [TaskScheduler][fluid.scheduler.TaskScheduler] doing
both jobs.

Run **one** scheduler per broker. Each scheduler keeps its own record of what it
last fired, in memory, with no coordination between processes, so two schedulers
on one broker queue every due task twice. Consumers are the part you replicate,
not the scheduler.

The two flags on [TaskManagerConfig][fluid.scheduler.TaskManagerConfig] let you
split the roles without changing class:

```python
TaskScheduler(schedule_tasks=False)  # behaves as a consumer
TaskScheduler(consume_tasks=False)   # schedules only, runs nothing
```

## In a FastAPI app

[task_manager_fastapi][fluid.scheduler.task_manager_fastapi] accepts any of the
three, and the choice decides what the app process does, because the task routes
themselves are identical in every case. `POST /tasks/{name}` queues a run
whichever manager is behind it.

When the manager is a worker, a consumer or a scheduler, it is added to the app
workers and starts and stops with the app: serving requests and running tasks
happen in the same process. When it is a plain
[TaskManager][fluid.scheduler.TaskManager], the app only gets startup and
shutdown hooks, enough to open and close the resources registered with
[add_async_context_manager][fluid.scheduler.TaskManager.add_async_context_manager],
and nothing consumes the queue.

So the same tasks module can be served by two entry points, an API that only
produces and a worker that schedules and executes:

```python
--8<-- "./examples/docs/task_managers.py"
```

Both entry points register the tasks, and they have to: a task must be in the
registry to be queued by name, and a consumer that receives a run for a task it
does not know logs an unknown task error. The dependencies, on the other hand,
are only needed where tasks actually run, which is why the producer above builds
none.

Deploy that pair as one API deployment scaled for traffic and one worker
deployment scaled for load, sharing a broker. For a single service doing both,
pass a [TaskScheduler][fluid.scheduler.TaskScheduler] and be done. See
[Extending the FastAPI App](task_fastapi.md) for reaching whichever manager you
chose from your own routes.

## Choosing

* Only queueing work, or executing it inline in tests and scripts, use
  [TaskManager][fluid.scheduler.TaskManager].
* Running queued work, with no schedules in the process, use
  [TaskConsumer][fluid.scheduler.TaskConsumer].
* Owning the clock, and usually consuming too, use
  [TaskScheduler][fluid.scheduler.TaskScheduler]. This is the default choice for
  a single-service application.

Whichever you pick, an application with [CPU bound tasks](tasks.md#cpu-bound-tasks)
needs [TaskManagerCLI][fluid.scheduler.cli.TaskManagerCLI] as its entry point,
because the subprocess that runs such a task builds its own task manager from it.
See [Setup for CPU bound tasks](task_app.md#setup-for-cpu-bound-tasks).
