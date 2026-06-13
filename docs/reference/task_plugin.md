# Task Manager Plugins

Plugins extend the [TaskManager][fluid.scheduler.TaskManager] with additional behaviour
by hooking into task lifecycle events.

A plugin implements the [TaskManagerPlugin][fluid.scheduler.TaskManagerPlugin] interface
and is registered via [TaskManager.with_plugin][fluid.scheduler.TaskManager.with_plugin].

```python
from fluid.scheduler import TaskScheduler, task_manager_fastapi
from fluid.scheduler.db import TaskDbPlugin

task_manager = TaskScheduler(...)
task_manager.with_plugin(TaskDbPlugin(db))
app = task_manager_fastapi(task_manager)
```

::: fluid.scheduler.TaskManagerPlugin

::: fluid.scheduler.db.TaskDbPlugin

## Accessing the plugin from a task

[get_db_plugin][fluid.scheduler.db.get_db_plugin] retrieves the registered
[TaskDbPlugin][fluid.scheduler.db.TaskDbPlugin] from the task manager state.
It is designed as a FastAPI dependency for route handlers, but can also be
called directly from within a task by passing `context.task_manager`:

```python
from fluid.scheduler import TaskRun, task
from fluid.scheduler.db import get_db_plugin, TaskHistoryQuery


@task()
async def report(context: TaskRun) -> None:
    db_plugin = get_db_plugin(context.task_manager)
    page = await db_plugin.get_history(TaskHistoryQuery(task="my-task", limit=10))
    for run in page.data:
        print(run.id, run.state)
```

::: fluid.scheduler.db.get_db_plugin

## History Models

The following models are used when querying task run history via
[TaskDbPlugin.get_history][fluid.scheduler.db.TaskDbPlugin.get_history]
or the HTTP endpoints.

They can be imported from `fluid.scheduler.db`:

```python
from fluid.scheduler.db import TaskHistoryQuery, TaskRunHistory, TaskRunHistoryPage
```

### Filtering by tags

The `tags` field of [TaskHistoryQuery][fluid.scheduler.db.TaskHistoryQuery]
filters runs by the tags of their [Task][fluid.scheduler.Task]. A run matches
when its task carries **at least one** of the supplied tags (OR semantics, the
same as the `tags` query parameter on the task list endpoint). Tags are
resolved against the live task registry at query time, so they always reflect
each task's *current* tags rather than the tags it had when the run executed.

When combined with the `task` filter, the two are applied together (AND): the
run's task must match the name *and* carry one of the tags. Tags that match no
registered task return an empty result.

::: fluid.scheduler.db.TaskHistoryQuery

::: fluid.scheduler.db.TaskRunHistory

::: fluid.scheduler.db.TaskRunHistoryPage
