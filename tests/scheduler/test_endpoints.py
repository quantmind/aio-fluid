import pytest

from fluid.scheduler.models import TaskInfo, TaskState
from fluid.utils.http_client import HttpResponseError
from tests.scheduler.tasks import TaskClient

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_get_tasks(cli: TaskClient) -> None:
    data = await cli.get(f"{cli.url}/tasks")
    assert len(data) == 4
    tasks = {task["name"]: TaskInfo(**task) for task in data}
    dummy = tasks["dummy"]
    assert dummy.name == "dummy"


async def test_get_tasks_status(cli: TaskClient) -> None:
    data = await cli.get(f"{cli.url}/tasks-status")
    assert data


async def test_run_task_404(cli: TaskClient) -> None:
    with pytest.raises(HttpResponseError):
        await cli.post(f"{cli.url}/tasks/whatever")


async def test_run_task(cli: TaskClient) -> None:
    task = await cli.get_task("dummy")
    assert task.last_run_end is None
    task = await cli.get_task("dummy")
    data = await cli.post(f"{cli.url}/tasks/dummy")
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
    data = await cli.post(f"{cli.url}/tasks/dummy")
    task = await cli.wait_for_task("dummy", last_run_end=task.last_run_end)
    assert task.enabled is False
    assert task.last_run_state == TaskState.aborted
    assert task.last_run_end is not None
