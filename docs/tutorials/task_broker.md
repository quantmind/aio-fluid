# Task Broker

A [TaskBroker][fluid.scheduler.TaskBroker] needs to implement three abstract methods

```python
  @abstractmethod
  async def queue_task(self, task_run: TaskRun) -> None:
      """Queue a task"""

  @abstractmethod
  async def get_task_run(self) -> Optional[TaskRun]:
      """Get a Task run from the task queue"""

  @abstractmethod
  async def queue_length(self) -> Dict[str, int]:
      """Length of task queues"""

  @abstractmethod
    async def update_task(self, task: Task, params: dict[str, Any]) -> TaskInfo:
        """Update a task dynamic parameters"""

  @abstractmethod
  async def close(self) -> None:
      """Close the broker on shutdown"""

  @abstractmethod
  def lock(self, name: str, timeout: float | None = None) -> Lock:
      """Create a lock"""
```

The library ships a Redis broker for convenience.

```python
from fluid.scheduler import Broker

redis_broker = Broker.from_url("redis://localhost:6349")
```

By default the broker uses the url provided in the `FLUID_BROKER_URL` environment variable and falls back to `redis://localhost:6349`.

```python
broker = Broker.from_url()
broker.url == "redis://localhost:6349"
```
