# Task Broker

A [TaskBroker][fluid.scheduler.TaskBroker] is responsible for queuing task runs and
storing task information. A subclass has to implement every abstract method below,
grouped here by the concern each one serves.

```python
  # queues

  @property
  @abstractmethod
  def task_queue_names(self) -> tuple[str, ...]:
      """Names of the task queues"""

  @abstractmethod
  async def queue_task(self, task_run: TaskRun) -> None:
      """Queue a task run"""

  @abstractmethod
  async def get_task_run(self, task_manager: TaskManager) -> TaskRun | None:
      """Get a Task run from the task queue"""

  @abstractmethod
  async def queue_length(self) -> dict[str, int]:
      """Length of task queues"""

  @abstractmethod
  async def clear_queue(self, *priorities: TaskPriority) -> dict[str, int]:
      """Clear task queues, returns number of removed items per priority"""

  # task information

  @abstractmethod
  async def get_tasks_info(self, *task_names: str) -> list[TaskInfo]:
      """List of TaskInfo objects"""

  @abstractmethod
  async def update_task(self, task: Task, params: dict[str, Any]) -> TaskInfo:
      """Update a task dynamic parameters"""

  # in-flight task runs, used to enforce max_concurrency

  @abstractmethod
  async def add_task_run(self, task_run: TaskRun) -> None:
      """Add a task run to the broker"""

  @abstractmethod
  async def remove_task_run(self, task_run: TaskRun) -> None:
      """Remove a task run from the broker"""

  @abstractmethod
  async def current_task_runs(self, task_name: str) -> int:
      """The number of current task runs for a given task_name"""

  # aborts

  @abstractmethod
  async def set_task_aborted(self, run_id: str, reason: str) -> None:
      """Signal that a task run was aborted, storing the reason"""

  @abstractmethod
  async def get_task_aborted(self, run_id: str) -> str | None:
      """Return the abort reason for a task run, or None if not aborted"""

  # task manager status

  @abstractmethod
  async def set_manager_status(self, manager_id: str, data: dict, ttl: int) -> None:
      """Store the status of a running task manager"""

  @abstractmethod
  async def get_all_manager_statuses(self) -> TaskManagersStatus:
      """Get statuses of all running task managers"""

  # locking and shutdown

  @abstractmethod
  def lock(self, name: str, timeout: float | None = None) -> Lock:
      """Create a lock"""

  @abstractmethod
  async def close(self) -> None:
      """Close the broker on shutdown"""
```

The return type of `lock` is currently the lock class of the `redis` client, so a broker
built on a different backend has to return an object satisfying that type. Only the async
context manager protocol is used at runtime.

The library ships a Redis broker for convenience.

```python
from fluid.scheduler import TaskBroker

redis_broker = TaskBroker.from_url("redis://localhost:6379")
```

By default the broker uses the url provided in the `FLUID_BROKER_URL` environment variable and falls back to `redis://localhost:6379`.

```python
broker = TaskBroker.from_url()
str(broker.url) == "redis://localhost:6379"
```
