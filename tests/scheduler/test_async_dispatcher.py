import asyncio

import pytest

from examples import tasks
from fluid.scheduler import TaskRun, TaskState
from tests.scheduler.tasks import redis_broker

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def failing_handler(task_run: TaskRun) -> None:
    raise RuntimeError("handler failed")


async def test_consumer_stops_when_async_dispatcher_dies() -> None:
    tm = tasks.task_scheduler(
        max_concurrent_tasks=1,
        schedule_tasks=False,
        stopping_grace_period=1,
    )
    tm.register_async_handler(TaskState.running, failing_handler)
    await redis_broker(tm).clear()
    await tm.startup()
    assert tm.is_running()

    await tm.queue("dummy", sleep=0.1)

    async with asyncio.timeout(5):
        await tm.wait_for_shutdown()
    assert tm.is_stopped()
