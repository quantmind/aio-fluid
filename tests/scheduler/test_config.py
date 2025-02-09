from pydantic import BaseModel

from fluid.scheduler import TaskConsumer
from tests.scheduler.tasks import task_application


def test_no_workers() -> None:
    task_consumer = TaskConsumer(max_concurrent_tasks=0)
    assert task_consumer.config.max_concurrent_tasks == 0


async def test_no_queues() -> None:
    task_consumer = TaskConsumer(broker_url="redis://localhost:7777?queues=")
    assert task_consumer.config.max_concurrent_tasks == 5
    assert task_consumer.broker.task_queue_names == ()
    assert await task_consumer.broker.queue_length() == {}


async def test_two_queues() -> None:
    task_consumer = TaskConsumer(
        broker_url="redis://localhost:7777?name=test&queues=medium,high"
    )
    assert task_consumer.config.max_concurrent_tasks == 5
    assert task_consumer.broker.task_queue_names == (
        "test-queue-medium",
        "test-queue-high",
    )


def test_params() -> None:
    tasks = list(task_application().registry.values())
    for task in tasks:
        assert task.params_model is not None
        assert issubclass(task.params_model, BaseModel)
        assert task.params_model is not BaseModel
        task.params_model().model_dump()
