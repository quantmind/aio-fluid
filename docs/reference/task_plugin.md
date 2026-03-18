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
