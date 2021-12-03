# Distrubuted Task Producer/Consumer

This module has a lightweight implementation of a distributed task producer (Scheduler) and consumer.
The middleware for distributing tasks can be configured via the Broker interface.
A redis broker is provided for convenience.

## Tasks

Tasks are standard python async functions decorated with the `task` or `cpu_task` decorators.

```python
from fluid.scheduler import task, TaskContext

@task
async def say_hi(ctx: TaskContext):
    return "Hi!"
```

There are three types of tasks implemented

* Simple concurrent tasks - they run concurrently with the task consumer - thy must be IO type tasks (no heavy CPU bound operations)
  ```python
    from fluid.scheduler import task, TaskContext

    @task
    async def fecth_data(ctx: TaskContext):
        # fetch data
        data = await http_cli.get("https://...")
        # trigger another task
        ctx.queue
    ```
