import os
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any, Callable, Dict, Iterable, NamedTuple, Optional, Tuple, Union
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


class UnknownTask(RuntimeError):
    pass


class TaskRegistry(Dict[str, Task]):
    def periodic(self) -> Iterable[Task]:
        for task in self.values():
            if task.schedule:
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
        """Get a Task run from the task queue"""

    async def close(self) -> None:
        """Close the broker on shutdown"""

    def new_uuid(self) -> str:
        return uuid4().hex

    def task_from_registry(self, task: Union[str, Task]) -> Task:
        if isinstance(task, Task):
            self.register_task(task)
        else:
            task_ = self.registry.get(task)
            if not task_:
                raise UnknownTask(task)
            task = task_
        return task

    def register_task(self, task: Task) -> None:
        self.registry[task.name] = task

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

    def task_queue_name(self, priority: TaskPriority) -> str:
        return f"{self.name}-queue-{priority.name}"

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


Broker.register_broker("redis", RedisBroker)
