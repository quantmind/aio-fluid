# Task Manager

The Task Manager is a component that manages the execution of tasks. It is the simplest way to run tasks and it is the base class for the [TaskConsumer][fluid.scheduler.TaskConsumer]
and the [TaskScheduler][fluid.scheduler.TaskScheduler].

It can be imported from `fluid.scheduler`:

```python
from fastapi.scheduler import TaskManager
```

The Task Manager is useful if you want to execute tasks in a synchronous or asynchronous way.


::: fluid.scheduler.TaskManager

::: fluid.scheduler.TaskManagerConfig

::: fluid.scheduler.consumer.TaskDispatcher
