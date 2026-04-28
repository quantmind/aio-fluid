# Task Manager Plugins

Plugins extend the [TaskManager][fluid.scheduler.TaskManager] with additional behaviour
by hooking into task lifecycle events.

A plugin implements the [TaskManagerPlugin][fluid.scheduler.TaskManagerPlugin] interface
and is registered via [TaskManager.with_plugin][fluid.scheduler.TaskManager.with_plugin].

```python
from fluid.scheduler import TaskScheduler, task_manager_fastapi
from fluid.scheduler.db import TaskDbPlugin, with_task_history_router

task_manager = TaskScheduler(...)
task_manager.with_plugin(TaskDbPlugin(db))
app = task_manager_fastapi(task_manager)
with_task_history_router(app)
```

::: fluid.scheduler.TaskManagerPlugin

::: fluid.scheduler.db.TaskDbPlugin

::: fluid.scheduler.db.with_task_history_router

## Accessing the plugin from a task

[get_db_plugin][fluid.scheduler.db.get_db_plugin] retrieves the registered
[TaskDbPlugin][fluid.scheduler.db.TaskDbPlugin] from the task manager state.
It is designed as a FastAPI dependency for route handlers, but can also be
called directly from within a task by passing `context.task_manager`:

```python
from fluid.scheduler import TaskRun, task
from fluid.scheduler.db import get_db_plugin, HistoryQuery


@task()
async def report(context: TaskRun) -> None:
    db_plugin = get_db_plugin(context.task_manager)
    page = await db_plugin.get_history(HistoryQuery(task="my-task", limit=10))
    for run in page.data:
        print(run.id, run.state)
```

::: fluid.scheduler.db.get_db_plugin

## History Models

The following models are used when querying task run history via
[TaskDbPlugin.get_history][fluid.scheduler.db.TaskDbPlugin.get_history]
or the HTTP endpoints added by [with_task_history_router][fluid.scheduler.db.with_task_history_router].

They can be imported from `fluid.scheduler.db`:

```python
from fluid.scheduler.db import HistoryQuery, TaskRunHistory, TaskRunHistoryPage
```

::: fluid.scheduler.db.HistoryQuery

::: fluid.scheduler.db.TaskRunHistory

::: fluid.scheduler.db.TaskRunHistoryPage
