from typing import cast

from pydantic import BaseModel

from examples import tasks as example_tasks
from fluid.scheduler import TaskConsumer, TaskManager
from fluid.scheduler.broker import RedisTaskBroker


def test_no_workers() -> None:
    task_consumer = TaskConsumer(max_concurrent_tasks=0)
    assert task_consumer.config.max_concurrent_tasks == 0


async def test_no_queues() -> None:
    task_consumer = TaskConsumer(broker_url="redis://localhost:7777?queues=")
    assert task_consumer.type == "task_consumer"
    assert task_consumer.config.max_concurrent_tasks == 5
    assert task_consumer.broker.task_queue_names == ()
    assert await task_consumer.broker.queue_length() == {}
    rb = cast(RedisTaskBroker, task_consumer.broker)
    assert rb.prefix == "{redis-task-broker}"


async def test_two_queues() -> None:
    task_consumer = TaskConsumer(
        broker_url="redis://localhost:7777?name=test&queues=medium,high"
    )
    assert task_consumer.config.max_concurrent_tasks == 5
    assert task_consumer.broker.task_queue_names == (
        "{test}-queue-medium",
        "{test}-queue-high",
    )


def test_params() -> None:
    tasks = list(example_tasks.task_scheduler().registry.values())
    for task in tasks:
        assert task.params_model is not None
        assert issubclass(task.params_model, BaseModel)
        assert task.params_model is not BaseModel
        task.params_model().model_dump()


def test_register_from_module() -> None:
    task_manager = TaskManager()
    task_manager.register_from_module(example_tasks)
    assert task_manager.registry
    assert task_manager.registry["dummy"]
