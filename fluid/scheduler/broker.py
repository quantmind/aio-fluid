import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)
from uuid import uuid4

from yarl import URL

from fluid import json
from fluid.redis import FluidRedis
from fluid.utils import microseconds

from .constants import TaskPriority, TaskState
from .task import Task
from .task_run import TaskRun

_brokers = {}

DEFAULT_BROKER_URL = "redis://localhost:6379/3"


class TaskError(RuntimeError):
    pass


class UnknownTask(TaskError):
    pass


class DisabledTask(TaskError):
    pass


@dataclass
class TaskInfo:
    name: str
    description: str
    priority: str
    schedule: Optional[str] = None
    enabled: bool = True
    last_run_end: Optional[int] = None
    last_run_duration: Optional[int] = None
    last_run_state: Optional[str] = None


class TaskRegistry(Dict[str, Task]):
    def periodic(self) -> Iterable[Task]:
        for task in self.values():
            yield task


class QueuedTask(NamedTuple):
    run_id: str
    task: str
    priority: Optional[TaskPriority]
    params: Dict[str, Any]


class Broker(ABC):
    def __init__(self, url: URL) -> None:
        self.url: URL = url
        self.registry: TaskRegistry = TaskRegistry()

    @abstractmethod
    async def queue_task(self, queued_task: QueuedTask) -> TaskRun:
        """Queue a task"""

    @abstractmethod
    async def get_task_run(self) -> Optional[TaskRun]:
        """Get a Task run from the task queue"""

    @abstractmethod
    async def queue_length(self) -> Dict[str, int]:
        """Length of task queues"""

    @abstractmethod
    async def get_tasks_info(self, *task_names: str) -> List[TaskInfo]:
        """List of TaskInfo objects"""

    @abstractmethod
    async def update_task(self, task: Task, params: dict) -> TaskInfo:
        """Update a task dynamic parameters"""

    async def close(self) -> None:
        """Close the broker on shutdown"""

    def new_uuid(self) -> str:
        return uuid4().hex

    async def filter_tasks(
        self, scheduled: Optional[bool] = None, enabled: Optional[bool] = None
    ) -> List[Task]:
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

    def task_from_registry(self, task: Union[str, Task]) -> Task:
        if isinstance(task, Task):
            self.register_task(task)
            task_ = task
        else:
            task_ = self.registry.get(task)
            if not task_:
                raise UnknownTask(task)
        return task_

    def register_task(self, task: Task) -> None:
        self.registry[task.name] = task

    async def enable_task(self, task_name: str, enable: bool = True) -> TaskInfo:
        """Enable or disable a registered task"""
        task = self.registry.get(task_name)
        if not task:
            raise UnknownTask(task_name)
        return await self.update_task(task, dict(enabled=enable))

    def task_run_from_data(self, data: Dict[str, Any]) -> TaskRun:
        """Build a TaskRun object from its metadata"""
        data = data.copy()
        name = data.pop("name")
        data["task"] = self.task_from_registry(name)
        return TaskRun(**data)

    def task_run_data(
        self, queued_task: QueuedTask, state: TaskState
    ) -> Dict[str, Any]:
        """Create a dictionary of metadata required by a task run

        This dictionary must be serializable by the broker
        """
        task = self.task_from_registry(queued_task.task)
        priority = queued_task.priority or task.priority
        return dict(
            id=queued_task.run_id,
            name=task.name,
            priority=priority.name,
            state=state.name,
            params=queued_task.params,
            queued=microseconds(),
        )

    @classmethod
    def from_url(cls, url: str = "") -> "Broker":
        url = url or os.getenv("SCHEDULER_BROKER_URL", DEFAULT_BROKER_URL)
        p = URL(url)
        Factory = _brokers.get(p.scheme)
        if not Factory:
            raise RuntimeError(f"Invalid broker {url}")
        return Factory(p)

    @classmethod
    def register_broker(cls, name: str, factory: Callable):
        _brokers[name] = factory


class RedisBroker(Broker):
    """A simple broker based on redis lists"""

    @cached_property
    def redis(self) -> FluidRedis:
        return FluidRedis(str(self.url.with_query({})), name=self.name)

    @property
    def name(self) -> str:
        return self.url.query.get("name", "redis-task-broker")

    @cached_property
    def task_queue_names(self) -> Tuple[str, ...]:
        return tuple(self.task_queue_name(p) for p in TaskPriority)

    def task_hash_name(self, name: str) -> str:
        return f"{self.name}-tasks-{name}"

    def task_queue_name(self, priority: TaskPriority) -> str:
        return f"{self.name}-queue-{priority.name}"

    async def get_tasks_info(self, *task_names: str) -> List[TaskInfo]:
        pipe = self.redis.cli.pipeline()
        names = task_names or self.registry
        requested_task_names = []
        for name in names:
            if name in self.registry:
                requested_task_names.append(name)
                pipe.hgetall(self.task_hash_name(name))
        tasks_info = await pipe.execute()
        return [
            self._decode_task(self.registry[name], task_info)
            for name, task_info in zip(requested_task_names, tasks_info)
        ]

    async def update_task(self, task: Task, params: dict) -> TaskInfo:
        pipe = self.redis.cli.pipeline()
        pipe.hset(
            self.task_hash_name(task.name),
            mapping={name: json.dumps(value) for name, value in params.items()},
        )
        pipe.hgetall(self.task_hash_name(task.name))
        _, info = await pipe.execute()
        return self._decode_task(task, info)

    async def queue_length(self) -> Dict[str, int]:
        pipe = self.redis.cli.pipeline()
        for name in self.task_queue_names:
            pipe.llen(name)
        result = await pipe.execute()
        return {p.name: r for p, r in zip(TaskPriority, result)}

    async def close(self) -> None:
        """Close the broker on shutdown"""
        await self.redis.close()

    async def get_task_run(self) -> Optional[TaskRun]:
        data = await self.redis.cli.brpop(self.task_queue_names, timeout=1)
        if data:
            data_str = data[1].decode("utf-8")
            return self.task_run_from_data(json.loads(data_str))
        return None

    async def queue_task(self, queued_task: QueuedTask) -> TaskRun:
        task = self.task_from_registry(queued_task.task)
        priority = queued_task.priority or task.priority
        data = self.task_run_data(queued_task, TaskState.queued)
        await self.redis.cli.lpush(self.task_queue_name(priority), json.dumps(data))
        return self.task_run_from_data(data)

    def _decode_task(self, task: Task, data: dict):
        info = {name.decode(): json.loads(value) for name, value in data.items()}
        return TaskInfo(
            name=task.name,
            description=task.description,
            schedule=str(task.schedule) if task.schedule else None,
            priority=task.priority.name,
            enabled=info.get("enabled", True),
            last_run_duration=info.get("last_run_duration"),
            last_run_end=info.get("last_run_end"),
            last_run_state=info.get("last_run_state"),
        )


Broker.register_broker("redis", RedisBroker)
