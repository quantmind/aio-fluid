import pytest

from fluid.scheduler import Consumer, Scheduler


async def test_scheduler(scheduler: Scheduler):
    assert scheduler
    assert scheduler.broker.registry
    assert "dummy" in scheduler.broker.registry


async def test_consumer(consumer: Consumer):
    assert consumer.broker.registry
    assert "dummy" in consumer.broker.registry
    assert consumer.num_concurrent_tasks == 0


async def test_dummy_execution(consumer: Consumer):
    task_run = consumer.execute("dummy")
    assert task_run.name == "dummy"
    await task_run.result
    assert task_run.end


async def test_dummy_queue(consumer: Consumer):
    task_run = await consumer.queue_and_wait("dummy")
    assert task_run.end


async def test_dummy_error(consumer: Consumer):
    task_run = await consumer.queue_and_wait("dummy", error=True)
    with pytest.raises(RuntimeError):
        await task_run.result
