import pytest

from fluid.scheduler.models import TaskInfo, TaskState
from fluid.utils.http_client import HttpResponseError
from fluid.utils.waiter import wait_for
from tests.scheduler.tasks import TaskClient

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_get_tasks(cli: TaskClient) -> None:
    data = await cli.get(f"{cli.url}/tasks")
    assert len(data) == 12
    tasks = {task["name"]: TaskInfo(**task) for task in data}
    dummy = tasks["dummy"]
    assert dummy.name == "dummy"


async def test_get_tasks_status(cli: TaskClient) -> None:
    async def check() -> bool:
        data = await cli.get(f"{cli.url}/tasks-status")
        return bool(data.get("workers"))

    await wait_for(check, timeout=3.0)
    data = await cli.get(f"{cli.url}/tasks-status")
    assert len(data["workers"]) == 1
    assert data["workers"][0]["kind"] == "task_scheduler"
    assert data["workers"][0]["status"]
    assert "queues" in data


async def test_get_task_404(cli: TaskClient) -> None:
    with pytest.raises(HttpResponseError):
        await cli.get(f"{cli.url}/tasks/whatever")


async def test_run_task_404(cli: TaskClient) -> None:
    with pytest.raises(HttpResponseError):
        await cli.post(f"{cli.url}/tasks/whatever")


async def test_run_task(cli: TaskClient) -> None:
    task = await cli.get_task("dummy")
    assert task.last_run_end is None
    task = await cli.get_task("dummy")
    data = await cli.post(f"{cli.url}/tasks/dummy", json=dict(sleep=0.2))
    assert data["task"] == "dummy"
    # wait for task
    task = await cli.wait_for_task("dummy")
    assert task.last_run_end is not None


async def test_patch_task_404(cli: TaskClient) -> None:
    with pytest.raises(HttpResponseError):
        await cli.patch(f"{cli.url}/tasks/whatever", json=dict(enabled=False))


async def test_patch_task(cli: TaskClient) -> None:
    data = await cli.patch(f"{cli.url}/tasks/dummy", json=dict(enabled=False))
    assert data["enabled"] is False
    task = await cli.get_task("dummy")
    assert task.enabled is False
    data = await cli.post(f"{cli.url}/tasks/dummy", json=dict(sleep=0.2))
    task = await cli.wait_for_task("dummy", last_run_end=task.last_run_end)
    assert task.enabled is False
    assert task.last_run_state == TaskState.aborted
    assert task.last_run_end is not None


async def test_ping(cli: TaskClient) -> None:
    data = await cli.post(f"{cli.url}/tasks/ping")
    assert data["task"] == "ping"


async def test_delete_queue(cli: TaskClient) -> None:
    data = await cli.delete(f"{cli.url}/tasks-queue")
    assert set(data.keys()) == {"high", "medium", "low"}
    assert all(isinstance(v, int) for v in data.values())


async def test_delete_queue_by_priority(cli: TaskClient) -> None:
    data = await cli.delete(f"{cli.url}/tasks-queue/high")
    assert set(data.keys()) == {"high"}
    assert isinstance(data["high"], int)
