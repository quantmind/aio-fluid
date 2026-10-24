import pytest
from yarl import URL

from fluid import settings
from fluid.scheduler import TaskScheduler
from fluid.scheduler.broker import RedisTaskBroker, TaskBroker

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
