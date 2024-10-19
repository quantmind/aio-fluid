# Task Consumer

The task consumer is a [TaskManager][fluid.scheduler.TaskManager] which is also a [Workers][fluid.utils.worker.Workers] that consumes tasks from the task queue and executes them. It can be imported from `fluid.scheduler`:

```python
from fastapi.scheduler import TaskConsumer
```

::: fluid.scheduler.TaskConsumer
