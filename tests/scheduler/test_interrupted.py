import asyncio

import pytest

from examples import tasks
from fluid.scheduler import TaskState
from fluid.scheduler.db import TaskDbPlugin
from tests.scheduler.tasks import redis_broker

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_task_run_interrupted(db_plugin: TaskDbPlugin) -> None:
    tm = tasks.task_scheduler(
        plugins=[db_plugin],
        max_concurrent_tasks=1,
        schedule_tasks=False,
        stopping_grace_period=1,
    )
    await redis_broker(tm).clear()
    await tm.startup()

    task_run = await tm.queue("dummy", sleep=100)

    async with asyncio.timeout(5):
        while await tm.broker.current_task_runs("dummy") == 0:
            await asyncio.sleep(0.05)

    assert await tm.broker.current_task_runs("dummy") == 1
    await tm.shutdown()

    task = await db_plugin.get_run(task_run.id)
    assert task.state == TaskState.interrupted
    assert task.end is not None
