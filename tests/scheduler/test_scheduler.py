import os
from dataclasses import dataclass, field
from typing import List

from aioredis import Redis

from fluid.scheduler import TaskConsumer, TaskRun, TaskScheduler
from fluid.utils import wait_for


@dataclass
class WaitFor:
    name: str
    times: int = 2
    runs: List[TaskRun] = field(default_factory=list)

    def __call__(self, task_run: TaskRun, _):
        if task_run.name == self.name:
            self.runs.append(task_run)


async def test_scheduler(task_scheduler: TaskScheduler):
    assert task_scheduler
    assert task_scheduler.broker.registry
    assert "dummy" in task_scheduler.registry
    assert "scheduled" in task_scheduler.registry


async def test_consumer(task_consumer: TaskConsumer):
    assert task_consumer.broker.registry
    assert "dummy" in task_consumer.broker.registry
    assert task_consumer.num_concurrent_tasks == 0


async def test_dummy_execution(task_consumer: TaskConsumer):
    task_run = task_consumer.execute("dummy")
    assert task_run.name == "dummy"
    await task_run.waiter
    assert task_run.end


async def test_dummy_queue(task_consumer: TaskConsumer):
    task_run = await task_consumer.queue_and_wait("dummy")
    assert task_run.end


async def test_dummy_error(task_consumer: TaskConsumer):
    task_run = await task_consumer.queue_and_wait("dummy", error=True)
    assert isinstance(task_run.exception, RuntimeError)


async def test_scheduled(task_consumer: TaskConsumer):
    handler = WaitFor(name="scheduled")
    task_consumer.register_handler("end.scheduled", handler)
    try:
        await wait_for(lambda: len(handler.runs) >= 2, timeout=3)
    finally:
        task_consumer.unregister_handler("end.handler")


async def test_cpubound_execution(task_consumer: TaskConsumer, redis: Redis):
    task_run = task_consumer.execute("cpu_bound")
    await task_run.waiter
    assert task_run.end
    assert task_run.result is None
    assert task_run.exception is None
    result = await redis.get(task_run.id)
    assert result
    assert int(result) != os.getpid()
