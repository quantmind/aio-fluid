import asyncio
from typing import AsyncIterator, cast

import pytest
from fastapi import FastAPI

from examples import tasks
from examples.db import get_db
from fluid.scheduler import TaskState
from fluid.scheduler.consumer import TaskConsumer
from fluid.scheduler.db import TaskDbPlugin
from fluid.scheduler.endpoints import get_task_manager, task_manager_fastapi
from tests.scheduler.conftest import start_fastapi

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest.fixture(scope="module")
def db_plugin() -> TaskDbPlugin:
    db = get_db()
    plugin = TaskDbPlugin(db)
    mig = db.migration()
    if not mig.db_create():
        mig.drop_all_schemas()
    mig.create_all()
    return plugin


@pytest.fixture(scope="module")
async def task_app_db(db_plugin: TaskDbPlugin) -> AsyncIterator[FastAPI]:
    task_manager = tasks.task_scheduler(
        plugins=[db_plugin],
        max_concurrent_tasks=2,
        schedule_tasks=False,
    )
    async with start_fastapi(task_manager_fastapi(task_manager)) as app:
        yield app


@pytest.fixture(scope="module")
async def task_manager_db(task_app_db: FastAPI) -> TaskConsumer:
    tm = cast(TaskConsumer, get_task_manager(task_app_db))
    await tm.broker.enable_task("add", enable=True)
    return tm


async def wait_for_row(
    db_plugin: TaskDbPlugin, run_id: str, timeout: float = 2.0
) -> None:
    table = db_plugin.db.tables[db_plugin.table_name]
    async with asyncio.timeout(timeout):
        while True:
            rows = (await db_plugin.db.db_select(table, dict(id=run_id))).fetchall()
            if rows and rows[0].end is not None:
                return
            await asyncio.sleep(0.05)


async def test_task_run_stored_on_success(
    task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    table = db_plugin.db.tables[db_plugin.table_name]
    task_run = await task_manager_db.queue_and_wait("dummy", timeout=5)
    assert task_run.state == TaskState.success

    await wait_for_row(db_plugin, task_run.id)
    rows = (await db_plugin.db.db_select(table, dict(id=task_run.id))).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row.name == "dummy"
    assert row.state == TaskState.success
    assert row.queued is not None
    assert row.start is not None
    assert row.end is not None
    assert row.params == {"sleep": 0.1, "error": False}


async def test_task_run_stored_on_failure(
    task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    table = db_plugin.db.tables[db_plugin.table_name]
    task_run = await task_manager_db.queue_and_wait("fast", sleep=5, timeout=5)
    assert task_run.state == TaskState.failure

    await wait_for_row(db_plugin, task_run.id)
    rows = (await db_plugin.db.db_select(table, dict(id=task_run.id))).fetchall()
    assert len(rows) == 1
    assert rows[0].state == TaskState.failure
    assert rows[0].end is not None


async def test_task_run_params_stored(
    task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    table = db_plugin.db.tables[db_plugin.table_name]
    task_run = await task_manager_db.queue_and_wait("add", timeout=5, a=3.0, b=4.0)
    assert task_run.state == TaskState.success

    await wait_for_row(db_plugin, task_run.id)
    rows = (await db_plugin.db.db_select(table, dict(id=task_run.id))).fetchall()
    assert len(rows) == 1
    assert rows[0].params == {"a": 3.0, "b": 4.0}


async def test_task_run_aborted(
    task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    table = db_plugin.db.tables[db_plugin.table_name]
    await task_manager_db.broker.enable_task("add", enable=False)
    try:
        task_run = await task_manager_db.queue_and_wait("add", timeout=5)
    finally:
        await task_manager_db.broker.enable_task("add", enable=True)
    assert task_run.state == TaskState.aborted

    await wait_for_row(db_plugin, task_run.id)
    rows = (await db_plugin.db.db_select(table, dict(id=task_run.id))).fetchall()
    assert len(rows) == 1
    assert rows[0].state == TaskState.aborted
