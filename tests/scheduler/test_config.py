from typing import cast

from pydantic import BaseModel

from examples import tasks as example_tasks
from fluid.scheduler import TaskConsumer, TaskManager, TaskRun, task
from fluid.scheduler.broker import RedisTaskBroker
from fluid.scheduler.models import EmptyParams


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
    for registered in tasks:
        assert registered.params_model is not None
        assert issubclass(registered.params_model, BaseModel)
        assert registered.params_model is not BaseModel
        params_model = registered.params_model
        if not any(f.is_required() for f in params_model.model_fields.values()):
            params_model().model_dump()


def test_register_from_module() -> None:
    task_manager = TaskManager()
    task_manager.register_from_module(example_tasks)
    assert task_manager.registry
    assert task_manager.registry["dummy"]


def test_register_from_module_with_tags() -> None:
    task_manager = TaskManager()
    task_manager.register_from_module(example_tasks, tags=["extra", "module"])
    assert task_manager.registry
    dummy = task_manager.registry["dummy"]
    assert {"extra", "module"} <= dummy.tags


def test_cpu_bount_params() -> None:
    cpu_bound = example_tasks.cpu_bound
    assert cpu_bound.params_model is example_tasks.Sleep


async def test_typed_deps_params() -> None:
    """Annotating the deps type does not break the params model inference."""

    @task
    async def typed_deps(ctx: TaskRun[example_tasks.Sleep, example_tasks.Deps]) -> None:
        """Task annotating both the params and the deps types."""

    assert typed_deps.params_model is example_tasks.Sleep


async def test_untyped_deps_params() -> None:
    """A single type parameter keeps working, the deps type defaults to Any."""

    @task
    async def untyped_deps(ctx: TaskRun[example_tasks.Sleep]) -> None:
        """Task annotating only the params type."""

    assert untyped_deps.params_model is example_tasks.Sleep


type AppTaskRun[P: BaseModel] = TaskRun[P, example_tasks.Deps]


async def test_aliased_task_run_params() -> None:
    """An application aliasing the task run to bind its deps keeps its params."""

    @task
    async def aliased(ctx: AppTaskRun[example_tasks.Sleep]) -> None:
        """Task annotated with the application alias."""

    @task
    async def aliased_no_params(ctx: AppTaskRun) -> None:
        """Task annotated with the bare application alias."""

    assert aliased.params_model is example_tasks.Sleep
    assert aliased_no_params.params_model is EmptyParams
