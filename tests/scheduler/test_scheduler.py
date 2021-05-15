from dataclasses import dataclass, field
from typing import List

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


async def test_scheduler(scheduler: TaskScheduler):
    assert scheduler
    assert scheduler.broker.registry
    assert "dummy" in scheduler.registry
    assert "scheduled" in scheduler.registry


async def test_consumer(consumer: TaskConsumer):
    assert consumer.broker.registry
    assert "dummy" in consumer.broker.registry
    assert consumer.num_concurrent_tasks == 0


async def test_dummy_execution(consumer: TaskConsumer):
    task_run = consumer.execute("dummy")
    assert task_run.name == "dummy"
    await task_run.waiter
    assert task_run.end


async def test_dummy_queue(consumer: TaskConsumer):
    task_run = await consumer.queue_and_wait("dummy")
    assert task_run.end


async def test_dummy_error(consumer: TaskConsumer):
    task_run = await consumer.queue_and_wait("dummy", error=True)
    assert isinstance(task_run.exception, RuntimeError)


async def test_scheduled(consumer: TaskConsumer):
    handler = WaitFor(name="scheduled")
    consumer.register_handler("end.scheduled", handler)
    try:
        await wait_for(lambda: len(handler.runs) >= 2, timeout=3)
    finally:
        consumer.unregister_handler("end.handler")
