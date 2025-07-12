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
from .models import TP, Task, TaskInfo, TaskInfoUpdate, TaskPriority, TaskRun

if TYPE_CHECKING:  # pragma: no cover
    from .consumer import TaskManager


_brokers: dict[str, type[TaskBroker]] = {}


def broker_url_from_env() -> URL:
    return URL(settings.BROKER_URL)


class TaskRegistry(dict[str, Task[TP]]):
    """A registry of tasks"""

    def periodic(self) -> Iterable[Task]:
        """Iterate over periodic tasks"""
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
    async def add_task_run(self, task_run: TaskRun) -> None:
        """Add a task run to the broker"""

    @abstractmethod
    async def remove_task_run(self, task_run: TaskRun) -> None:
        """Remove a task run from the broker"""

    @abstractmethod
    async def current_task_runs(self, task_name: str) -> int:
        """The number of current task runs for a given task_name"""

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

    async def enable_task(self, task: str | Task, enable: bool = True) -> TaskInfo:
        """Enable or disable a registered task"""
        task_ = self.task_from_registry(task)
        return await self.update_task(task_, dict(enabled=enable))

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

    def __init__(self, url: URL) -> None:
        super().__init__(url)
        self.redis = FluidRedis.create(str(self.url.with_query({})), name=self.name)

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

    @property
    def prefix(self) -> str:
        """Prefix for the keys - use {...} for redis hash tags"""
        return "{%s}" % self.name

    def task_hash_name(self, name: str) -> str:
        """name of the key containing the task info"""
        return f"{self.prefix}-tasks-{name}"

    def task_runs_set_name(self, name: str) -> str:
        """name of the key containing the task runs info"""
        return f"{self.prefix}-task-runs-{name}"

    def task_queue_name(self, priority: TaskPriority) -> str:
        return f"{self.prefix}-queue-{priority}"

    async def clear(self) -> int:
        pipe = self.redis_cli.pipeline()
        async for key in self.redis_cli.scan_iter(f"{self.prefix}-*"):
            pipe.delete(key)
        r = await pipe.execute()
        return len(r)

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
            for name, task_info in zip(requested_task_names, tasks_info, strict=False)
        ]

    async def update_task(self, task: Task, params: dict[str, Any]) -> TaskInfo:
        pipe = self.redis_cli.pipeline()
        info = json.loads(TaskInfoUpdate(**params).model_dump_json(exclude_unset=True))
        update = {name: json.dumps(value) for name, value in info.items()}
        key = self.task_hash_name(task.name)
        pipe.hset(key, mapping=update)
        pipe.hgetall(key)
        _, data = await pipe.execute()
        return self._decode_task(task, data)

    async def queue_length(self) -> dict[str, int]:
        if self.task_queue_names:
            pipe = self.redis_cli.pipeline()
            for name in self.task_queue_names:
                pipe.llen(name)
            result = await pipe.execute()
            return dict(zip(TaskPriority, result, strict=False))
        return {}

    async def add_task_run(self, task_run: TaskRun) -> None:
        """Add a task run to the broker"""
        await self.redis_cli.sadd(
            self.task_runs_set_name(task_run.name),
            task_run.id,
        )

    async def remove_task_run(self, task_run: TaskRun) -> None:
        """Remove a task run from the broker"""
        await self.redis_cli.srem(
            self.task_runs_set_name(task_run.name),
            task_run.id,
        )

    async def current_task_runs(self, task_name: str) -> int:
        return await self.redis_cli.scard(self.task_runs_set_name(task_name))

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
                task = self.task_from_registry(data["task"])
                data.update(
                    task=task,
                    params=task.params_model(**data["params"]),
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
