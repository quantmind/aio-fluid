import asyncio

from fluid.scheduler import Consumer, Scheduler, TaskRun


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
    waiter = asyncio.Future()

    def waitfor(task_run: TaskRun):
        if task_run.name == "dummy":
            waiter.set_result(task_run)

    consumer.register_handler("end.test", waitfor)
    consumer.queue("dummy")
    task_run = await waiter
    consumer.unregister_handler("end.test")
    assert task_run.end
