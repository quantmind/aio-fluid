import asyncio
import os
from dataclasses import dataclass, field
from typing import Any

import pytest
from redis.asyncio import Redis

from fluid.scheduler import (
    TaskPriority,
    TaskRun,
    TaskScheduler,
    TaskState,
)
from fluid.scheduler.errors import UnknownTaskError
from fluid.utils.waiter import wait_for


@dataclass
class WaitFor:
    name: str
    times: int = 2
    runs: list[TaskRun] = field(default_factory=list)

    def __call__(self, task_run: TaskRun, _: Any) -> None:
        if task_run.name == self.name:
            self.runs.append(task_run)


def test_scheduler_manager(task_scheduler: TaskScheduler) -> None:
    assert task_scheduler
    assert task_scheduler.broker.registry
    assert "dummy" in task_scheduler.registry
    assert "scheduled" in task_scheduler.registry
    with pytest.raises(UnknownTaskError):
        task_scheduler.broker.task_from_registry("bbdjchbjch")


async def test_queue_length(task_scheduler: TaskScheduler) -> None:
    ql = await task_scheduler.broker.queue_length()
    assert len(ql) == 3
    for p in TaskPriority:
        assert ql[p.name] >= 0


async def test_execute(task_scheduler: TaskScheduler) -> None:
    task_run = await task_scheduler.execute("dummy")
    assert task_run.name == "dummy"
    assert task_run.end


async def test_dummy_queue(task_scheduler: TaskScheduler) -> None:
    assert await task_scheduler.queue_and_wait("dummy", sleep=0.2) == 0.2


async def test_dummy_error(task_scheduler: TaskScheduler) -> None:
    with pytest.raises(RuntimeError):
        await task_scheduler.queue_and_wait("dummy", error=True)


@pytest.mark.flaky
async def test_dummy_rate_limit(task_scheduler: TaskScheduler) -> None:
    s1, s2 = await asyncio.gather(
        task_scheduler.queue_and_wait("dummy", sleep=0.53),
        task_scheduler.queue_and_wait("dummy", sleep=0.52),
    )
    assert s1 == 0.53
    assert s2 is None
    assert task_scheduler.num_concurrent_tasks == 0


@pytest.mark.flaky
async def test_scheduled(task_scheduler: TaskScheduler) -> None:
    handler = WaitFor(name="scheduled")
    task_scheduler.register_handler("end.scheduled", handler)
    try:
        await wait_for(lambda: len(handler.runs) >= 2, timeout=3)
    finally:
        task_scheduler.unregister_handler("end.handler")


@pytest.mark.flaky
async def test_cpubound_execution(
    task_scheduler: TaskScheduler, redis: Redis  # type: ignore
) -> None:
    task_run = task_scheduler.execute("cpu_bound")
    await task_run.waiter
    assert task_run.end
    assert task_run.result is None
    assert task_run.exception is None
    result = await redis.get(task_run.id)
    assert result
    assert int(result) != os.getpid()


async def test_task_info(task_scheduler: TaskScheduler) -> None:
    with pytest.raises(UnknownTaskError):
        await task_scheduler.broker.enable_task("hfgfhgfhfh")
    info = await task_scheduler.broker.enable_task("scheduled")
    assert info.enabled is True
    assert info.name == "scheduled"
    assert info.schedule == "every(0:00:01)"
    assert info.description == "A simple scheduled task"


async def test_disabled_execution(task_scheduler: TaskScheduler) -> None:
    info = await task_scheduler.broker.enable_task("disabled", enable=False)
    assert info.enabled is False
    assert info.name == "disabled"
    task_run = task_scheduler.execute("disabled")
    assert task_run.name == "disabled"
    await task_run.waiter
    assert task_run.end
    assert task_run.state == TaskState.aborted.name
