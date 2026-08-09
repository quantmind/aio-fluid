# Task Dependencies

Production tasks rarely run in isolation: they need a database manager, an HTTP
client, a cache. The [TaskManager][fluid.scheduler.TaskManager] carries a single
`deps` object for exactly this, and every task run can reach it.

## Passing dependencies

Group what your tasks need into one object and pass it to the task manager.
Any object will do, a dataclass is a good fit:

```python
--8<-- "./examples/docs/task_deps.py"
```

Inside a task the dependencies are available as
[TaskRun.deps][fluid.scheduler.TaskRun.deps]. Annotate the second type parameter
of [TaskRun][fluid.scheduler.TaskRun] to have them typed, as in
`TaskRun[Quote, Deps]` above: without it `deps` is typed as `Any` and you get no
completion or type checking.

Both the params and the deps parameters are optional, so `TaskRun`,
`TaskRun[Quote]` and `TaskRun[Quote, Deps]` are all valid annotations.

## Resource lifecycle

Dependencies that hold a resource needing a startup and a shutdown, a connection
pool for instance, should not be opened by each task. Register them with
[TaskManager.add_async_context_manager][fluid.scheduler.TaskManager.add_async_context_manager]
and the task manager enters them when it starts and exits them when it stops:

```python
scheduler.add_async_context_manager(deps.http_client)
```

## Dependencies are not shared with CPU bound tasks

A task declared with `cpu_bound=True` does not run in the consumer process. It
is executed by a separate process, or by a Kubernetes Job in a cluster, which
builds its own task manager from the command line entry point. Its dependencies
are therefore constructed again, in that process, and nothing is shared with the
consumer.

Two consequences worth keeping in mind:

* Dependencies must be cheap to construct, because the cost is paid on every
  run of a CPU bound task.
* Anything held in memory by a dependency, a cache or an open connection, is not
  visible to a CPU bound task. Use the database or the broker to pass state
  across the process boundary.

See [K8s Jobs](task_k8s.md) for how CPU bound tasks are dispatched.

## Defaults and plugins

When no `deps` is passed the task manager creates an empty
[State](https://www.starlette.io/applications/#storing-state-on-the-app-instance),
the same namespace object starlette uses for `app.state`, so attributes can be
set on it after construction:

```python
scheduler = TaskScheduler()
scheduler.deps.db_manager = db_manager
```

This works but is untyped and offers no protection against two components
choosing the same attribute name. Prefer passing a typed object.

`deps` belongs to your application. A separate `state` namespace, also a
starlette [State](https://www.starlette.io/applications/#storing-state-on-the-app-instance),
is reserved for [plugins][fluid.scheduler.TaskManagerPlugin], which use it to
store their own data on the task manager without colliding with your
dependencies. See [Plugins](task_app.md#plugins).
