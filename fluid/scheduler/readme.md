# Distrubuted Task Producer/Consumer

This module has a lightweight implementation of a distributed task producer (TaskScheduler) and consumer (TaskConsumer).
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

There are two types of tasks implemented

* **Simple concurrent tasks** - they run concurrently with the task consumer - thy must be IO type tasks (no heavy CPU bound operations)

  ```python
    from fluid.scheduler import task, TaskContext

    @task
    async def fecth_data(ctx: TaskContext):
        # fetch data
        data = await http_cli.get("https://...")
        data_id = await datastore_cli.stote(data)
        # trigger another task
        ctx.task_manager.queue("heavy_calculation", data_id=data_id)
  ```

* **CPU bound tasks** - they run on a subprocess

  ```python
    from fluid.scheduler import cpu_task, TaskContext

    @cpu_task
    async def heavy_calculation(ctx: TaskContext):
        # perform some heavy calculation
        data = await datastore_cli.get(ctx.params["data_id"])
        ...
        # trigger another task
        ctx.task_manager.queue("fetch_data")
  ```

Both tasks can be periodically scheduled via the `schedule` keyword argument:

```python
from datetime import timedelta
from fluid.scheduler import task, TaskContext, every

@task(schedule=every(timedelta(seconds=1)))
async def scheduled(context: TaskContext) -> str:
    await asyncio.sleep(0.1)
    return "OK"
```


## Broker

A Task broker needs to implement three abstract methods
```python
  @abstractmethod
  async def queue_task(self, queued_task: QueuedTask) -> TaskRun:
      """Queue a task"""

  @abstractmethod
  async def get_task_run(self) -> Optional[TaskRun]:
      """Get a Task run from the task queue"""

  @abstractmethod
  async def queue_length(self) -> Dict[str, int]:
      """Length of task queues"""
```

The library ships a Redis broker for convenience.

```python
from fluid.scheduler import Broker

redis_broker = Broker.from_url("redis://localhost:6349")
```
