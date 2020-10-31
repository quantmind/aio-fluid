import asyncio
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Callable, Dict, Optional, Union
from uuid import uuid4

from yarl import URL

from fluid.redis import RedisPubSub
from fluid.utils import milliseconds

from .task import Task

_brokers = {}

DEFAULT_BROKER_URL = "redis://localhost:6379/3"


class UnknownTask(RuntimeError):
    pass


@dataclass
class TaskRun:
    id: str
    queued: int
    task: Task
    params: Dict[str, Any]
    start: int = 0
    end: int = 0
    result: asyncio.Future = field(default_factory=asyncio.Future)

    @property
    def name(self) -> str:
        return self.task.name


class Broker(ABC):
    def __init__(self, url: URL) -> None:
        self.url: URL = url
        self.registry: Dict[str, Task] = {}

    @abstractmethod
    async def queue_task(
        self, run_is: str, task: Union[str, Task], params: Dict[str, Any]
    ) -> TaskRun:
        """Queue a task"""

    @abstractmethod
    async def get_task_run(self) -> Optional[TaskRun]:
        """Get a Task run from the task queue"""

    @abstractmethod
    async def get_tasks(self) -> Dict[str, Dict]:
        """Load tasks"""

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
        data = data.copy()
        name = data.pop("name")
        return TaskRun(task=self.task_from_registry(name), **data)

    def task_run_data(
        self, run_id: str, task: Union[str, Task], params: Dict[str, Any]
    ) -> Dict[str, Any]:
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

    async def get_task_run(self) -> Optional[TaskRun]:
        pub = await self.redis.pub()
        data = await pub.brpop(self.task_queue_name, timeout=1)
        if data:
            data_str = data[1].decode("utf-8")
            return self.task_run_from_data(json.loads(data_str))
        return None

    async def get_tasks(self) -> Dict[str, Dict]:
        return {}

    async def queue_task(
        self, run_id: str, task: Union[str, Task], params: Dict[str, Any]
    ) -> TaskRun:
        pub = await self.redis.pub()
        data = self.task_run_data(run_id, task, params)
        await pub.lpush(self.task_queue_name, json.dumps(data))
        return self.task_run_from_data(data)


Broker.register_broker("redis", RedisBroker)
