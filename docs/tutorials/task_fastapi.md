# Extending the FastAPI App

[task_manager_fastapi][fluid.scheduler.task_manager_fastapi] returns a plain
FastAPI app with the task routes mounted on it. It is meant to be extended: your
own routes can live in the same app and reach the same
[TaskManager][fluid.scheduler.TaskManager], with the same dependencies and the
same resources the tasks use.

## One app for tasks and routes

Pass your own app with the `app` argument and the task routes are added to it,
rather than to a new one:

```python
--8<-- "./examples/docs/task_fastapi.py"
```

Building the app in a factory function keeps it usable from
[TaskManagerCLI][fluid.scheduler.cli.TaskManagerCLI], which is what runs the app
and, for [CPU bound tasks](task_app.md#setup-for-cpu-bound-tasks), what a task
subprocess runs too.

## Reaching the task manager

The task manager is stored on the app state as `app.state.task_manager`. Rather
than reading the attribute, use the accessors in `fluid.scheduler.endpoints`:

```python
from fluid.scheduler.endpoints import TaskManagerDep, get_task_manager
```

[TaskManagerDep][fluid.scheduler.endpoints.TaskManagerDep] is an annotated
FastAPI dependency, so a route asks for the task manager by declaring it as an
argument:

```python
@router.get("/queue-size")
async def queue_size(task_manager: TaskManagerDep) -> dict[str, int]:
    return await task_manager.broker.queue_length()
```

[get_task_manager(app)][fluid.scheduler.endpoints.get_task_manager] does the
same outside a request, where there is an app but no request to depend on, in a
test fixture or a startup hook for instance.

## Injecting the dependencies

[TaskManager.deps][fluid.scheduler.TaskManager] is typed as `Any`, because the
library does not know what your application puts in it. Wrap the cast in a
dependency of your own once, and every route gets the dependencies fully typed:

```python
def get_deps(task_manager: TaskManagerDep) -> Deps:
    return cast(Deps, task_manager.deps)


DepsDep = Annotated[Deps, Depends(get_deps)]
```

A route then declares `deps: DepsDep` and works with a `Deps` object, not with
`Any`. See [Task Dependencies](task_deps.md) for how `deps` is built and passed
to the task manager.

The resources are shared, not merely visible. A route and a task run hold the
same HTTP client instance, the same connection pool, the same cache. Anything
registered with
[TaskManager.add_async_context_manager][fluid.scheduler.TaskManager.add_async_context_manager]
is entered when the app starts and exited when it stops, so by the time a route
runs the client is already open, and there is nothing to open or close per
request.

This holds only for routes in the same process as the task manager. A
`cpu_bound=True` task builds its own dependencies in its own process and shares
nothing with the app, as described in
[Dependencies are not shared with CPU bound tasks](task_deps.md#dependencies-are-not-shared-with-cpu-bound-tasks).

## Plugin state

Plugins keep their data in a separate namespace,
[TaskManager.state][fluid.scheduler.TaskManager], so they never collide with the
`deps` your application owns. A plugin that serves routes reads it back through
the same task manager dependency. The database plugin does exactly this:

```python
from fluid.scheduler.db import TaskDbPluginDep
```

so its `/tasks-history` routes reach the plugin without the application having
to wire anything. See [Plugins](task_app.md#plugins).

## Workers

When the task manager is a worker, a
[TaskConsumer][fluid.scheduler.TaskConsumer] or a
[TaskScheduler][fluid.scheduler.TaskScheduler],
[task_manager_fastapi][fluid.scheduler.task_manager_fastapi] adds it to the app
workers, which start and stop with the app. `WorkersDep` from
`fluid.tools_fastapi` gives a route access to that worker set, to report their
status for a health endpoint for instance:

```python
from fluid.tools_fastapi import WorkersDep
```
