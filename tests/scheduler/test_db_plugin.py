import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncIterator, cast

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from examples import tasks
from examples.db import get_db
from fluid.scheduler import TaskState
from fluid.scheduler.consumer import TaskConsumer
from fluid.scheduler.db import TaskDbPlugin, get_db_plugin, with_task_history_router
from fluid.scheduler.endpoints import get_task_manager, task_manager_fastapi
from fluid.utils.http_client import HttpResponseError
from tests.scheduler.tasks import TaskClient, redis_broker, start_fastapi

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
    await redis_broker(task_manager).clear()
    app = task_manager_fastapi(task_manager)
    with_task_history_router(app)
    async with start_fastapi(app) as app:
        yield app


@pytest.fixture(scope="module")
async def cli_db(task_app_db: FastAPI) -> AsyncIterator[TaskClient]:
    base_url = TaskClient().url
    async with AsyncClient(
        transport=ASGITransport(app=task_app_db), base_url=base_url
    ) as session:
        async with TaskClient(url=base_url, session=session) as client:
            yield client


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
    assert row.params == {"sleep": 0.1, "error": False, "abort": False}


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


async def test_task_run_params_with_datetime(
    task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    table = db_plugin.db.tables[db_plugin.table_name]
    dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    task_run = await task_manager_db.queue_and_wait("datetime_task", timeout=5, dt=dt)
    assert task_run.state == TaskState.success

    await wait_for_row(db_plugin, task_run.id)
    rows = (await db_plugin.db.db_select(table, dict(id=task_run.id))).fetchall()
    assert len(rows) == 1
    assert rows[0].params == {"dt": "2024-01-15T12:00:00Z"}


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


async def test_task_run_aborted_via_context(
    task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    task_run = await task_manager_db.execute("dummy", abort=True)
    assert task_run.state == TaskState.aborted
    await wait_for_row(db_plugin, task_run.id)
    row = await get_db_plugin(task_manager_db).get_run(task_run.id)
    assert row.state == TaskState.aborted


async def get_history(cli_db: TaskClient, **params: Any) -> list[dict]:
    page = await cli_db.get(f"{cli_db.url}/task-history", params=params or None)
    assert "data" in page
    assert "cursor" in page
    return page["data"]


async def test_get_history(
    cli_db: TaskClient, task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    task_run = await task_manager_db.queue_and_wait("dummy", timeout=5)
    await wait_for_row(db_plugin, task_run.id)
    data = await get_history(cli_db)
    assert len(data) > 0
    assert any(item["id"] == task_run.id for item in data)


async def test_get_history_pagination(
    cli_db: TaskClient, task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    task_run = await task_manager_db.queue_and_wait("dummy", timeout=5)
    await wait_for_row(db_plugin, task_run.id)
    page = await cli_db.get(f"{cli_db.url}/task-history", params={"limit": 2})
    assert len(page["data"]) == 2
    assert page["cursor"]
    next_page = await cli_db.get(
        f"{cli_db.url}/task-history", params={"cursor": page["cursor"]}
    )
    assert next_page["data"]
    assert {item["id"] for item in page["data"]}.isdisjoint(
        {item["id"] for item in next_page["data"]}
    )


async def test_get_history_filter_by_name(
    cli_db: TaskClient, task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    task_run = await task_manager_db.queue_and_wait("dummy", timeout=5)
    await wait_for_row(db_plugin, task_run.id)
    data = await get_history(cli_db, task="dummy")
    assert all(item["task"] == "dummy" for item in data)
    data_other = await get_history(cli_db, task="add")
    assert all(item["task"] == "add" for item in data_other)


async def test_get_history_filter_by_state(
    cli_db: TaskClient, task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    task_run = await task_manager_db.queue_and_wait("dummy", timeout=5)
    await wait_for_row(db_plugin, task_run.id)
    data = await get_history(cli_db, state="success")
    assert all(item["state"] == "success" for item in data)


async def test_get_history_filter_by_start(
    cli_db: TaskClient, task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    before = datetime.now(tz=timezone.utc)
    task_run = await task_manager_db.queue_and_wait("dummy", timeout=5)
    await wait_for_row(db_plugin, task_run.id)
    data = await get_history(cli_db, start=before.isoformat())
    assert any(item["id"] == task_run.id for item in data)


async def test_skip_db_tag_not_persisted(
    task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    table = db_plugin.db.tables[db_plugin.table_name]
    task_run = await task_manager_db.queue_and_wait("ping", timeout=5)
    assert task_run.state == TaskState.success
    rows = (await db_plugin.db.db_select(table, dict(id=task_run.id))).fetchall()
    assert rows == []


async def test_get_run(
    cli_db: TaskClient, task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    task_run = await task_manager_db.queue_and_wait("dummy", timeout=5)
    await wait_for_row(db_plugin, task_run.id)
    data = await cli_db.get(f"{cli_db.url}/task-history/{task_run.id}")
    assert data["id"] == task_run.id
    assert data["task"] == "dummy"
    assert data["state"] == "success"


async def test_get_run_404(cli_db: TaskClient) -> None:
    with pytest.raises(HttpResponseError):
        await cli_db.get(f"{cli_db.url}/task-history/nonexistent-run-id")


async def test_get_history_filter_by_end(
    cli_db: TaskClient, task_manager_db: TaskConsumer, db_plugin: TaskDbPlugin
) -> None:
    task_run = await task_manager_db.queue_and_wait("dummy", timeout=5)
    await wait_for_row(db_plugin, task_run.id)
    after = datetime.now(tz=timezone.utc)
    data = await get_history(cli_db, end=after.isoformat())
    assert any(item["id"] == task_run.id for item in data)
    data_empty = await get_history(cli_db, end="2000-01-01T00:00:00Z")
    assert data_empty == []
