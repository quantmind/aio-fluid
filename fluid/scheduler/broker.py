import asyncio
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Callable, Dict, Iterable, Optional, Union
from uuid import uuid4

from yarl import URL

from fluid.redis import RedisPubSub
from fluid.utils import milliseconds

from .task import Task

_brokers = {}

DEFAULT_BROKER_URL = "redis://localhost:6379/3"


class UnknownTask(RuntimeError):
    pass


class TaskRegistry(Dict[str, Task]):
    def periodic(self) -> Iterable[Task]:
        for task in self.values():
            if task.schedule:
                yield task


@dataclass
class TaskRun:
    id: str
    queued: int
    task: Task
    params: Dict[str, Any]
    start: int = 0
    end: int = 0
    waiter: asyncio.Future = field(default_factory=asyncio.Future)

    @property
    def name(self) -> str:
        return self.task.name

    @property
    def name_id(self) -> str:
        return f"{self.task.name}.{self.id}"

    @property
    def exception(self) -> Optional[Exception]:
        if self.waiter.done():
            return self.waiter.exception()

    @property
    def result(self):
        if self.waiter.done() and not self.exception:
            return self.waiter.result()


class Broker(ABC):
    def __init__(self, url: URL) -> None:
        self.url: URL = url
        self.registry: TaskRegistry = TaskRegistry()

    @abstractmethod
    async def queue_task(
        self, run_is: str, task: Union[str, Task], params: Dict[str, Any]
    ) -> TaskRun:
        """Queue a task"""

    @abstractmethod
    async def get_task_run(self) -> Optional[TaskRun]:
        """Get a Task run from the task queue"""

    async def close(self) -> None:
        """Close the broker on shutdown"""

    def new_uuid(self) -> str:
        return uuid4().hex

    def task_from_registry(self, task: Union[str, Task]) -> Task:
        if isinstance(task, Task):
            self.register_task(task)
        else:
            task = self.registry.get(task)
            if not task:
                raise UnknownTask(task)
        return task

    def register_task(self, task: Task) -> None:
        self.registry[task.name] = task

    def task_run_from_data(self, data: Dict[str, Any]) -> TaskRun:
        """Build a TaskRun object from its metadata"""
        data = data.copy()
        name = data.pop("name")
        return TaskRun(task=self.task_from_registry(name), **data)

    def task_run_data(
        self, run_id: str, task: Union[str, Task], params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a dictionary of metadata required by a task run

        This dictionary must be serializable by the broker
        """
        task = self.task_from_registry(task)
        return dict(id=run_id, name=task.name, params=params, queued=milliseconds())

    @classmethod
    def from_env(cls) -> "Broker":
        url = os.getenv("SCHEDULER_BROKER_URL", DEFAULT_BROKER_URL)
        p = URL(url)
        Factory = _brokers.get(p.scheme)
        if not Factory:
            raise RuntimeError(f"Invalid broker {url}")
        return Factory(p)

    @classmethod
    def register_broker(cls, name: str, factory: Callable):
        _brokers[name] = factory


class RedisBroker(Broker):
    @cached_property
    def redis(self) -> RedisPubSub:
        return RedisPubSub(str(self.url.with_query({})), name=self.task_queue_name)

    @property
    def name(self) -> str:
        return self.url.query.get("name", "redis-task-broker")

    @property
    def task_queue_name(self) -> str:
        return f"{self.name}-queue"

    async def close(self) -> None:
        """Close the broker on shutdown"""
        await self.redis.close()

    async def get_task_run(self) -> Optional[TaskRun]:
        redis = await self.redis.pool()
        data = await redis.brpop(self.task_queue_name, timeout=1)
        if data:
            data_str = data[1].decode("utf-8")
            return self.task_run_from_data(json.loads(data_str))
        return None

    async def queue_task(
        self, run_id: str, task: Union[str, Task], params: Dict[str, Any]
    ) -> TaskRun:
        redis = await self.redis.pool()
        data = self.task_run_data(run_id, task, params)
        await redis.lpush(self.task_queue_name, json.dumps(data))
        return self.task_run_from_data(data)


Broker.register_broker("redis", RedisBroker)
