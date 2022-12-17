import asyncio
import os
from dataclasses import dataclass, field
from typing import List

import pytest

from fluid.redis import Redis
from fluid.scheduler import TaskConsumer, TaskRun, TaskScheduler
from fluid.scheduler.broker import UnknownTask
from fluid.scheduler.constants import TaskPriority, TaskState
from fluid.utils import wait_for


@dataclass
class WaitFor:
    name: str
    times: int = 2
    runs: List[TaskRun] = field(default_factory=list)

    def __call__(self, task_run: TaskRun, _):
        if task_run.name == self.name:
            self.runs.append(task_run)


@pytest.mark.flaky
def test_scheduler(task_scheduler: TaskScheduler):
    assert task_scheduler
    assert task_scheduler.broker.registry
    assert "dummy" in task_scheduler.registry
    assert "scheduled" in task_scheduler.registry
    with pytest.raises(UnknownTask):
        task_scheduler.broker.task_from_registry("bbdjchbjch")


async def test_queue_length(task_consumer: TaskConsumer):
    ql = await task_consumer.broker.queue_length()
    assert len(ql) == 3
    for p in TaskPriority:
        assert ql[p.name] >= 0


async def test_consumer(task_consumer: TaskConsumer):
    assert task_consumer.broker.registry
    assert "dummy" in task_consumer.broker.registry


async def test_dummy_execution(task_consumer: TaskConsumer):
    task_run = task_consumer.execute("dummy")
    assert task_run.name == "dummy"
    await task_run.waiter
    assert task_run.end


async def test_dummy_queue(task_consumer: TaskConsumer):
    assert await task_consumer.queue_and_wait("dummy", sleep=0.2) == 0.2


async def test_dummy_error(task_consumer: TaskConsumer):
    with pytest.raises(RuntimeError):
        await task_consumer.queue_and_wait("dummy", error=True)


@pytest.mark.flaky
async def test_dummy_rate_limit(task_consumer: TaskConsumer):
    s1, s2 = await asyncio.gather(
        task_consumer.queue_and_wait("dummy", sleep=0.53),
        task_consumer.queue_and_wait("dummy", sleep=0.52),
    )
    assert s1 == 0.53
    assert s2 is None
    assert task_consumer.num_concurrent_tasks == 0


async def test_scheduled(task_consumer: TaskConsumer):
    handler = WaitFor(name="scheduled")
    task_consumer.register_handler("end.scheduled", handler)
    try:
        await wait_for(lambda: len(handler.runs) >= 2, timeout=3)
    finally:
        task_consumer.unregister_handler("end.handler")


@pytest.mark.flaky
async def test_cpubound_execution(task_consumer: TaskConsumer, redis: Redis):
    task_run = task_consumer.execute("cpu_bound")
    await task_run.waiter
    assert task_run.end
    assert task_run.result is None
    assert task_run.exception is None
    result = await redis.get(task_run.id)
    assert result
    assert int(result) != os.getpid()


async def test_task_info(task_consumer: TaskConsumer):
    with pytest.raises(UnknownTask):
        await task_consumer.broker.enable_task("hfgfhgfhfh")
    info = await task_consumer.broker.enable_task("scheduled")
    assert info.enabled is True
    assert info.name == "scheduled"
    assert info.schedule == "every(0:00:01)"
    assert info.description == "A simple scheduled task"


async def test_disabled_execution(task_consumer: TaskConsumer):
    info = await task_consumer.broker.enable_task("disabled", enable=False)
    assert info.enabled is False
    assert info.name == "disabled"
    task_run = task_consumer.execute("disabled")
    assert task_run.name == "disabled"
    await task_run.waiter
    assert task_run.end
    assert task_run.state == TaskState.aborted.name
