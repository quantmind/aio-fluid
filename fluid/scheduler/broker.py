from __future__ import annotations

import json
from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterable
from uuid import uuid4

from redis.asyncio import Redis
from redis.asyncio.lock import Lock
from yarl import URL

from fluid import settings
from fluid.utils.redis import FluidRedis

from .errors import UnknownTaskError
from .models import Task, TaskInfo, TaskInfoUpdate, TaskPriority, TaskRun

if TYPE_CHECKING:  # pragma: no cover
    from .consumer import TaskManager


_brokers: dict[str, type[TaskBroker]] = {}


def broker_url_from_env() -> URL:
    return URL(settings.BROKER_URL)


class TaskRegistry(dict[str, Task]):
    def periodic(self) -> Iterable[Task]:
        for task in self.values():
            yield task


class TaskBroker(ABC):
    def __init__(self, url: URL) -> None:
        self.url: URL = url
        self.registry: TaskRegistry = TaskRegistry()

    @property
    @abstractmethod
    def task_queue_names(self) -> tuple[str, ...]:
        """Names of the task queues"""

    @abstractmethod
    async def queue_task(self, task_run: TaskRun) -> None:
        """Queue a task"""

    @abstractmethod
    async def get_task_run(self, task_manager: TaskManager) -> TaskRun | None:
        """Get a Task run from the task queue"""

    @abstractmethod
    async def queue_length(self) -> dict[str, int]:
        """Length of task queues"""

    @abstractmethod
    async def get_tasks_info(self, *task_names: str) -> list[TaskInfo]:
        """List of TaskInfo objects"""

    @abstractmethod
    async def update_task(self, task: Task, params: dict[str, Any]) -> TaskInfo:
        """Update a task dynamic parameters"""

    @abstractmethod
    async def close(self) -> None:
        """Close the broker on shutdown"""

    @abstractmethod
    def lock(self, name: str, timeout: float | None = None) -> Lock:
        """Create a lock"""

    def new_uuid(self) -> str:
        return uuid4().hex

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

    def task_from_registry(self, task: str | Task) -> Task:
        if isinstance(task, Task):
            self.register_task(task)
            return task
        else:
            if task_ := self.registry.get(task):
                return task_
            raise UnknownTaskError(task)

    def register_task(self, task: Task) -> None:
        self.registry[task.name] = task

    async def enable_task(self, task_name: str, enable: bool = True) -> TaskInfo:
        """Enable or disable a registered task"""
        task = self.registry.get(task_name)
        if not task:
            raise UnknownTaskError(task_name)
        return await self.update_task(task, dict(enabled=enable))

    @classmethod
    def from_url(cls, url: str = "") -> TaskBroker:
        p = URL(url or broker_url_from_env())
        if factory := _brokers.get(p.scheme):
            return factory(p)
        raise RuntimeError(f"Invalid broker {p}")

    @classmethod
    def register_broker(cls, name: str, factory: type[TaskBroker]) -> None:
        _brokers[name] = factory


class RedisTaskBroker(TaskBroker):
    """A simple task broker based on redis lists"""

    @cached_property
    def redis(self) -> FluidRedis:
        return FluidRedis.create(str(self.url.with_query({})), name=self.name)

    @property
    def redis_cli(self) -> Redis[bytes]:
        return self.redis.redis_cli

    @property
    def name(self) -> str:
        return self.url.query.get("name", "redis-task-broker")

    @cached_property
    def task_queue_names(self) -> tuple[str, ...]:
        priorities = self.url.query.get("queues")
        if priorities is None:
            return tuple(self.task_queue_name(p) for p in TaskPriority)
        elif priorities:
            return tuple(
                self.task_queue_name(TaskPriority[p]) for p in priorities.split(",")
            )
        else:
            return ()

    def task_hash_name(self, name: str) -> str:
        return f"{self.name}-tasks-{name}"

    def task_queue_name(self, priority: TaskPriority) -> str:
        return f"{self.name}-queue-{priority.name}"

    async def get_tasks_info(self, *task_names: str) -> list[TaskInfo]:
        pipe = self.redis_cli.pipeline()
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

    async def update_task(self, task: Task, params: dict[str, Any]) -> TaskInfo:
        pipe = self.redis_cli.pipeline()
        info = json.loads(TaskInfoUpdate(**params).model_dump_json())
        pipe.hset(
            self.task_hash_name(task.name),
            mapping={name: json.dumps(value) for name, value in info.items()},
        )
        pipe.hgetall(self.task_hash_name(task.name))
        _, info = await pipe.execute()
        return self._decode_task(task, info)

    async def queue_length(self) -> dict[str, int]:
        if self.task_queue_names:
            pipe = self.redis_cli.pipeline()
            for name in self.task_queue_names:
                pipe.llen(name)
            result = await pipe.execute()
            return dict(zip(TaskPriority, result))
        return {}

    async def close(self) -> None:
        """Close the broker on shutdown"""
        await self.redis.close()

    async def get_task_run(self, task_manager: TaskManager) -> TaskRun | None:
        if self.task_queue_names:
            if redis_data := await self.redis_cli.brpop(
                self.task_queue_names,
                timeout=1,
            ):
                data = json.loads(redis_data[1])
                data.update(
                    task=self.task_from_registry(data["task"]),
                    task_manager=task_manager,
                )
                return TaskRun(**data)
        return None

    async def queue_task(self, task_run: TaskRun) -> None:
        await self.redis_cli.lpush(
            self.task_queue_name(task_run.priority),
            task_run.model_dump_json(),
        )

    def lock(self, name: str, timeout: float | None = None) -> Lock:
        return self.redis_cli.lock(name, timeout=timeout)

    def _decode_task(self, task: Task, data: dict[bytes, Any]) -> TaskInfo:
        info = {name.decode(): json.loads(value) for name, value in data.items()}
        return task.info(**info)


TaskBroker.register_broker("redis", RedisTaskBroker)
TaskBroker.register_broker("rediss", RedisTaskBroker)
