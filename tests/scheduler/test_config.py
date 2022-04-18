from fluid.scheduler import TaskConsumer


def test_no_workers():
    task_consumer = TaskConsumer(max_concurrent_tasks=0)
    assert task_consumer.config.max_concurrent_tasks == 0


async def test_no_queues():
    task_consumer = TaskConsumer(broker_url="redis://localhost:7777?queues=")
    assert task_consumer.config.max_concurrent_tasks == 10
    assert task_consumer.broker.task_queue_names == ()
    assert await task_consumer.broker.queue_length() == {}


async def test_two_queues():
    task_consumer = TaskConsumer(
        broker_url="redis://localhost:7777?name=test&queues=medium,high"
    )
    assert task_consumer.config.max_concurrent_tasks == 10
    assert task_consumer.broker.task_queue_names == (
        "test-queue-medium",
        "test-queue-high",
    )
