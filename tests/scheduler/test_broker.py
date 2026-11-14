import time

import pytest
from yarl import URL

from fluid import settings
from fluid.scheduler import TaskScheduler
from fluid.scheduler.broker import RedisTaskBroker, TaskBroker
from tests.scheduler.tasks import redis_broker

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_registry_periodic(task_scheduler: TaskScheduler) -> None:
    periodic = list(task_scheduler.registry.periodic())
    assert periodic
    assert {t.name for t in periodic} == set(task_scheduler.registry)


async def test_filter_tasks_by_enabled(task_scheduler: TaskScheduler) -> None:
    broker = task_scheduler.broker
    await broker.enable_task("add", enable=False)
    try:
        enabled = {t.name for t in await broker.filter_tasks(enabled=True)}
        disabled = {t.name for t in await broker.filter_tasks(enabled=False)}
    finally:
        await broker.enable_task("add")
    assert "add" not in enabled
    assert disabled == {"add"}
    assert enabled | disabled == set(task_scheduler.registry)


async def test_from_url_invalid_scheme() -> None:
    with pytest.raises(RuntimeError, match="Invalid broker"):
        TaskBroker.from_url("unknown://localhost:1234")


async def test_manager_statuses_without_managers() -> None:
    url = URL(settings.BROKER_URL).update_query(name="no-managers-test")
    broker = RedisTaskBroker(url)
    try:
        await broker.clear()
        statuses = await broker.get_all_manager_statuses()
        assert statuses.workers == []
        assert set(statuses.queues.values()) == {0}
    finally:
        await broker.close()


async def test_current_task_runs(task_scheduler: TaskScheduler) -> None:
    broker = redis_broker(task_scheduler)
    task_run = task_scheduler.create_task_run("dummy")
    key = broker.task_runs_key("dummy")
    assert await broker.current_task_runs("dummy") == 0
    await broker.add_task_run(task_run)
    assert await broker.current_task_runs("dummy") == 1
    # the set expires with its latest run
    timeout = task_run.task.timeout_seconds
    assert 0 < await broker.redis_cli.ttl(key) <= timeout + 10
    # a run orphaned by a crashed task manager is dropped once expired
    await broker.redis_cli.zadd(key, {"orphan": time.time() - 1})
    assert await broker.current_task_runs("dummy") == 1
    assert await broker.redis_cli.zscore(key, "orphan") is None
    await broker.remove_task_run(task_run)
    assert await broker.current_task_runs("dummy") == 0
    assert not await broker.redis_cli.exists(key)


async def test_task_params_read_once(task_scheduler: TaskScheduler) -> None:
    broker = task_scheduler.broker
    await broker.set_task_params("params-run", '{"a": 1}', ttl=60)
    assert await broker.pop_task_params("params-run") == '{"a": 1}'
    assert await broker.pop_task_params("params-run") is None
