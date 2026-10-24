import heapq
from datetime import timedelta

import pytest
from pydantic import BaseModel, field_serializer
from yarl import URL

from fluid import settings
from fluid.scheduler import TaskRun, TaskScheduler, TaskState, task
from fluid.scheduler.models import K8sConfig
from fluid.utils.dates import utcnow
from fluid.utils.waiter import wait_for
from tests.scheduler.tasks import redis_broker

pytestmark = pytest.mark.asyncio(loop_scope="module")


class JsonUnfriendly(BaseModel):
    value: int = 1

    @field_serializer("value", when_used="json")
    def _no_json(self, value: int) -> int:
        raise ValueError("value cannot be serialized to json")


@task
async def json_unfriendly(ctx: TaskRun[JsonUnfriendly]) -> None:
    """A task whose params cannot be dumped to json"""


@task
async def json_unfriendly_aborting(ctx: TaskRun[JsonUnfriendly]) -> None:
    """A task whose params cannot be dumped to json and which aborts"""
    ctx.abort("stop here")


class CallableTask:
    """A task defined by a callable instance"""

    async def __call__(self, ctx: TaskRun) -> None:
        pass


async def test_no_consume_tasks() -> None:
    url = URL(settings.BROKER_URL).update_query(name="no-consume-test")
    tm = TaskScheduler(
        broker_url=str(url),
        consume_tasks=False,
        schedule_tasks=False,
        sleep_millis=1,
    )
    broker = redis_broker(tm)
    tm.register_task(json_unfriendly)
    try:
        await broker.clear()
        await tm.queue("json_unfriendly")
        await tm._consume_tasks("task-worker-1")
        # the queued run is left untouched
        assert sum((await broker.queue_length()).values()) == 1
    finally:
        await broker.clear()
        await broker.close()


async def test_consumed_run_not_ready_is_re_queued(
    task_scheduler: TaskScheduler,
) -> None:
    task_run = task_scheduler.create_task_run("dummy")
    task_run = task_run.model_copy(
        update=dict(execute_after=utcnow() + timedelta(seconds=60))
    )
    heap = task_scheduler._in_process_queue._heap

    def in_process_entries() -> list:
        return [e for e in heap if isinstance(e[2], TaskRun) and e[2].id == task_run.id]

    await task_scheduler.broker.queue_task(task_run)
    await wait_for(lambda: len(in_process_entries()) == 1, timeout=5)
    # remove it so it is not executed later
    for entry in in_process_entries():
        heap.remove(entry)
    heapq.heapify(heap)


async def test_params_json_dump_failure_still_runs(
    task_scheduler: TaskScheduler,
) -> None:
    task_run = await task_scheduler.queue_and_wait(json_unfriendly, timeout=5)
    assert task_run.state == TaskState.success


async def test_task_run_deps(task_scheduler: TaskScheduler) -> None:
    task_run = task_scheduler.create_task_run("dummy")
    assert task_run.deps is task_scheduler.deps


async def test_task_run_named_lock(task_scheduler: TaskScheduler) -> None:
    task_run = task_scheduler.create_task_run("dummy")
    lock = task_run.lock(timeout=1, name="extra")
    assert lock.name == "tasks:dummy:extra"
    async with lock:
        assert await lock.locked()


async def test_get_k8s_config_default(task_scheduler: TaskScheduler) -> None:
    dummy = task_scheduler.registry["dummy"]
    assert dummy.k8s_config is None
    assert dummy.get_k8s_config() == K8sConfig()


async def test_task_from_callable_instance() -> None:
    callable_task = task(CallableTask())
    assert callable_task.name == "CallableTask"
    assert callable_task.description == "A task defined by a callable instance"


async def test_params_json_dump_failure_then_abort() -> None:
    """The consumer survives a run that aborts after its params failed
    to dump to json, and does not leave the run in flight"""
    url = URL(settings.BROKER_URL).update_query(name="json-abort-test")
    tm = TaskScheduler(broker_url=str(url), schedule_tasks=False, sleep_millis=1)
    broker = redis_broker(tm)
    tm.register_task(json_unfriendly_aborting)
    try:
        await broker.clear()
        await tm.queue("json_unfriendly_aborting")
        await tm._consume_tasks("task-worker-1")
        assert await broker.current_task_runs("json_unfriendly_aborting") == 0
    finally:
        await broker.clear()
        await broker.close()
