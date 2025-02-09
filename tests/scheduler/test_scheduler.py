import asyncio
import os
from dataclasses import dataclass, field

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

pytestmark = pytest.mark.asyncio(loop_scope="module")


@dataclass
class WaitFor:
    name: str
    times: int = 2
    runs: list[TaskRun] = field(default_factory=list)

    def __call__(self, task_run: TaskRun) -> None:
        if task_run.name == self.name:
            self.runs.append(task_run)


async def test_scheduler_manager(task_scheduler: TaskScheduler) -> None:
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
    task_run = await task_scheduler.execute("dummy", sleep=0.2)
    assert task_run.is_done


async def test_dummy_error(task_scheduler: TaskScheduler) -> None:
    with pytest.raises(RuntimeError):
        await task_scheduler.execute("dummy", error=True)


async def test_dummy_rate_limit(task_scheduler: TaskScheduler) -> None:
    s1, s2 = await asyncio.gather(
        task_scheduler.queue_and_wait("dummy", sleep=2, timeout=10),
        task_scheduler.queue_and_wait("dummy", sleep=1, timeout=10),
    )
    assert task_scheduler.num_concurrent_tasks_for("dummy") == 0
    assert s1.is_done
    assert s2.is_done
    assert s1.state is TaskState.rate_limited or s2.state is TaskState.rate_limited


@pytest.mark.flaky
async def test_scheduled(task_scheduler: TaskScheduler) -> None:
    handler = WaitFor(name="scheduled")
    task_scheduler.dispatcher.register_handler("end.scheduled", handler)
    try:
        await wait_for(lambda: len(handler.runs) >= 2, timeout=3)
    finally:
        task_scheduler.dispatcher.unregister_handler("end.handler")


@pytest.mark.flaky
async def test_cpubound_execution(
    task_scheduler: TaskScheduler, redis: Redis  # type: ignore
) -> None:
    task_run = await task_scheduler.queue_and_wait("cpu_bound")
    assert task_run.end
    result = await redis.get(task_run.id)
    assert result
    assert int(result) != os.getpid()


async def test_task_info(task_scheduler: TaskScheduler) -> None:
    with pytest.raises(UnknownTaskError):
        await task_scheduler.broker.enable_task("hfgfhgfhfh")
    info = await task_scheduler.broker.enable_task("scheduled")
    assert info.enabled is True
    assert info.name == "scheduled"
    assert info.schedule == "every(0:00:02)"
    assert info.description == "A simple scheduled task"


async def test_disabled_execution(task_scheduler: TaskScheduler) -> None:
    info = await task_scheduler.broker.enable_task("add", enable=False)
    assert info.enabled is False
    assert info.name == "add"
    task_run = await task_scheduler.queue_and_wait(
        "add",
        priority=TaskPriority.high,
        a=3,
        b=4,
    )
    assert task_run.name == "add"
    assert task_run.params.a == 3
    assert task_run.params.b == 4
    assert task_run.end
    assert task_run.state == TaskState.aborted.name


@dataclass
class AsyncHandler:
    task_run: TaskRun | None = None

    async def __call__(self, task_run: TaskRun) -> None:
        await asyncio.sleep(0.1)
        self.task_run = task_run


async def test_async_handler(task_scheduler: TaskScheduler) -> None:
    handler = AsyncHandler()
    task_scheduler.register_async_handler("running.test", handler)
    task_run = await task_scheduler.queue_and_wait("dummy")
    assert task_run.state == TaskState.success
    await wait_for(lambda: handler.task_run is not None)
    assert handler.task_run
    assert handler.task_run.state == TaskState.running
    assert task_scheduler.unregister_async_handler("running.test") is handler
