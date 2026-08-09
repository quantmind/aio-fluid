# Task Broker

It can be imported from `fluid.scheduler`:

```python
from fluid.scheduler import TaskBroker
```

## fluid.scheduler.TaskBroker

```python
TaskBroker(url)
```

Bases: `ABC`

Abstract base class for task brokers

A TaskBroker is responsible for queuing tasks & storing task information

Source code in `fluid/scheduler/broker.py`

```python
def __init__(self, url: URL) -> None:
    self.url: Annotated[URL, Doc("Broker URL")] = url
    self.registry: Annotated[TaskRegistry, Doc("Task registry")] = TaskRegistry()
```

### url

```python
url = url
```

Broker URL

### registry

```python
registry = TaskRegistry()
```

Task registry

### task_queue_names

```python
task_queue_names
```

Names of the task queues

### queue_task

```python
queue_task(task_run)
```

Queue a task run

This method is called by the TaskManager when a task run is ready to be executed. The broker is responsible for adding the task run to the appropriate queue based on its priority.

| PARAMETER  | DESCRIPTION                  |
| ---------- | ---------------------------- |
| `task_run` | Task run **TYPE:** `TaskRun` |

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def queue_task(self, task_run: Annotated[TaskRun, Doc("Task run")]) -> None:
    """Queue a task run

    This method is called by the [TaskManager][fluid.scheduler.TaskManager] when
    a task run is ready to be executed.
    The broker is responsible for adding the task run to the appropriate
    queue based on its priority.
    """
```

### get_task_run

```python
get_task_run(task_manager)
```

Get a Task run from the task queue

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def get_task_run(self, task_manager: TaskManager) -> TaskRun | None:
    """Get a Task run from the task queue"""
```

### queue_length

```python
queue_length()
```

Length of task queues

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def queue_length(self) -> dict[str, int]:
    """Length of task queues"""
```

### clear_queue

```python
clear_queue(*priorities)
```

Clear task queues, returns number of removed items per priority

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def clear_queue(self, *priorities: TaskPriority) -> dict[str, int]:
    """Clear task queues, returns number of removed items per priority"""
```

### set_manager_status

```python
set_manager_status(manager_id, data, ttl)
```

Store the status of a running task manager

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def set_manager_status(self, manager_id: str, data: dict, ttl: int) -> None:
    """Store the status of a running task manager"""
```

### get_all_manager_statuses

```python
get_all_manager_statuses()
```

Get statuses of all running task managers

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def get_all_manager_statuses(self) -> TaskManagersStatus:
    """Get statuses of all running task managers"""
```

### get_tasks_info

```python
get_tasks_info(*task_names)
```

List of TaskInfo objects

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def get_tasks_info(self, *task_names: str) -> list[TaskInfo]:
    """List of TaskInfo objects"""
```

### update_task

```python
update_task(task, params)
```

Update a task dynamic parameters

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def update_task(self, task: Task, params: dict[str, Any]) -> TaskInfo:
    """Update a task dynamic parameters"""
```

### add_task_run

```python
add_task_run(task_run)
```

Add a task run to the broker

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def add_task_run(self, task_run: TaskRun) -> None:
    """Add a task run to the broker"""
```

### remove_task_run

```python
remove_task_run(task_run)
```

Remove a task run from the broker

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def remove_task_run(self, task_run: TaskRun) -> None:
    """Remove a task run from the broker"""
```

### current_task_runs

```python
current_task_runs(task_name)
```

The number of current task runs for a given task_name

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def current_task_runs(self, task_name: str) -> int:
    """The number of current task runs for a given task_name"""
```

### set_task_aborted

```python
set_task_aborted(run_id, reason)
```

Signal that a task run was aborted, storing the reason

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def set_task_aborted(self, run_id: str, reason: str) -> None:
    """Signal that a task run was aborted, storing the reason"""
```

### get_task_aborted

```python
get_task_aborted(run_id)
```

Return the abort reason for a task run, or None if not aborted

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def get_task_aborted(self, run_id: str) -> str | None:
    """Return the abort reason for a task run, or None if not aborted"""
```

### close

```python
close()
```

Close the broker on shutdown

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
async def close(self) -> None:
    """Close the broker on shutdown"""
```

### lock

```python
lock(name, timeout=None)
```

Create a lock

Source code in `fluid/scheduler/broker.py`

```python
@abstractmethod
def lock(self, name: str, timeout: float | None = None) -> Lock:
    """Create a lock"""
```

### new_uuid

```python
new_uuid()
```

Source code in `fluid/scheduler/broker.py`

```python
def new_uuid(self) -> str:
    return uuid4().hex
```

### filter_tasks

```python
filter_tasks(scheduled=None, enabled=None)
```

Source code in `fluid/scheduler/broker.py`

```python
async def filter_tasks(
    self,
    scheduled: bool | None = None,
    enabled: bool | None = None,
) -> list[Task]:
    task_info = await self.get_tasks_info()
    task_map = {info.name: info for info in task_info}
    tasks = []
    for task in self.registry.values():
        if scheduled is not None and bool(task.schedule) is not scheduled:
            continue
        if enabled is not None and task_map[task.name].enabled is not enabled:
            continue
        tasks.append(task)
    return tasks
```

### task_from_registry

```python
task_from_registry(task)
```

Source code in `fluid/scheduler/broker.py`

```python
def task_from_registry(self, task: str | Task) -> Task:
    if isinstance(task, Task):
        self.register_task(task)
        return task
    else:
        if task_ := self.registry.get(task):
            return task_
        raise UnknownTaskError(task)
```

### register_task

```python
register_task(task)
```

Source code in `fluid/scheduler/broker.py`

```python
def register_task(self, task: Task) -> None:
    self.registry[task.name] = task
```

### enable_task

```python
enable_task(task, enable=True)
```

Enable or disable a registered task

Source code in `fluid/scheduler/broker.py`

```python
async def enable_task(self, task: str | Task, enable: bool = True) -> TaskInfo:
    """Enable or disable a registered task"""
    task_ = self.task_from_registry(task)
    return await self.update_task(task_, dict(enabled=enable))
```

### from_url

```python
from_url(url='')
```

Source code in `fluid/scheduler/broker.py`

```python
@classmethod
def from_url(cls, url: str = "") -> TaskBroker:
    p = URL(url or broker_url_from_env())
    if factory := _brokers.get(p.scheme):
        return factory(p)
    raise RuntimeError(f"Invalid broker {p}")
```

### register_broker

```python
register_broker(name, factory)
```

Source code in `fluid/scheduler/broker.py`

```python
@classmethod
def register_broker(cls, name: str, factory: type[TaskBroker]) -> None:
    _brokers[name] = factory
```
