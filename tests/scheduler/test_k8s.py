"""Tests for fluid.scheduler.k8s_job — all K8s API calls are mocked."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fluid.scheduler.errors import TaskRunError
from fluid.scheduler.k8s_job import get_job_name, run_on_k8s_job
from fluid.scheduler.models import K8sConfig, K8sResourceRequirements

pytestmark = pytest.mark.asyncio(loop_scope="function")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_ctx(
    name: str = "heavy_calc",
    run_id: str = "abc1234xyz",
    k8s_config: K8sConfig | None = None,
    container_name: str = "main",
    command: list[str] | None = None,
    env: list | None = None,
    image: str = "myapp:latest",
) -> MagicMock:
    ctx = MagicMock()
    ctx.name = name
    ctx.id = run_id
    ctx.params.model_dump_json.return_value = "{}"

    cfg = k8s_config or K8sConfig(
        namespace="workers",
        deployment="fluid-task",
        container=container_name,
        sleep=0,  # no sleep in tests
    )
    ctx.task.get_k8s_config.return_value = cfg
    ctx.task.k8s_config = None

    container = MagicMock()
    container.name = container_name
    container.command = (
        command if command is not None else ["python", "-m", "myapp", "serve"]
    )
    container.env = env if env is not None else []
    container.image = image

    deployment = MagicMock()
    deployment.spec.template.spec.containers = [container]

    ctx._container = container
    ctx._deployment = deployment
    ctx._cfg = cfg
    return ctx


def make_k8s_mocks(ctx: MagicMock, *, succeeded: int = 1, failed: int | None = None):
    """Return a context manager that patches all K8s API calls."""
    deployment = ctx._deployment

    job_status = MagicMock()
    job_status.status.succeeded = succeeded
    job_status.status.failed = failed

    mock_v1 = AsyncMock()
    mock_v1.read_namespaced_deployment.return_value = deployment

    mock_batch = AsyncMock()
    mock_batch.create_namespaced_job.return_value = MagicMock(status="created")
    mock_batch.read_namespaced_job_status.return_value = job_status

    @asynccontextmanager
    async def fake_api_client():
        yield MagicMock()

    patches = [
        patch("fluid.scheduler.k8s_job.config.load_incluster_config"),
        patch("fluid.scheduler.k8s_job.ApiClient", side_effect=fake_api_client),
        patch("fluid.scheduler.k8s_job.client.AppsV1Api", return_value=mock_v1),
        patch("fluid.scheduler.k8s_job.client.BatchV1Api", return_value=mock_batch),
    ]
    return patches, mock_v1, mock_batch


# ---------------------------------------------------------------------------
# get_job_name
# ---------------------------------------------------------------------------


async def test_get_job_name_format() -> None:
    ctx = MagicMock()
    ctx.name = "heavy_calculation"
    ctx.id = "abc1234xyz"
    name = get_job_name(ctx)
    # should be slugified and contain task name + short id
    assert name.startswith("task-heavy-calculation-")
    assert "abc1234" in name


async def test_get_job_name_max_length() -> None:
    ctx = MagicMock()
    ctx.name = "a-very-long-task-name-that-exceeds-normal-limits-for-kubernetes"
    ctx.id = "z" * 40
    assert len(get_job_name(ctx)) <= 63


# ---------------------------------------------------------------------------
# run_on_k8s_job — happy path
# ---------------------------------------------------------------------------


async def test_run_on_k8s_job_success() -> None:
    ctx = make_ctx()
    patches, mock_v1, mock_batch = make_k8s_mocks(ctx, succeeded=1)

    with patches[0], patches[1], patches[2], patches[3]:
        await run_on_k8s_job(ctx)

    cfg = ctx._cfg
    mock_v1.read_namespaced_deployment.assert_awaited_once_with(
        cfg.deployment, cfg.namespace
    )
    mock_batch.create_namespaced_job.assert_awaited_once()
    mock_batch.read_namespaced_job_status.assert_awaited()


async def test_job_args_contain_task_name_and_run_id() -> None:
    ctx = make_ctx(name="my_task", run_id="run-99")
    patches, _, mock_batch = make_k8s_mocks(ctx, succeeded=1)

    with patches[0], patches[1], patches[2], patches[3]:
        await run_on_k8s_job(ctx)

    call_kwargs = mock_batch.create_namespaced_job.call_args
    job = call_kwargs.args[1]  # second positional arg is the V1Job
    container = job.spec.template.spec.containers[0]
    assert "my_task" in container.args
    assert "run-99" in container.args
    assert "exec" in container.args


async def test_serve_suffix_stripped_from_command() -> None:
    ctx = make_ctx(command=["python", "-m", "myapp", "serve"])
    patches, _, mock_batch = make_k8s_mocks(ctx, succeeded=1)

    with patches[0], patches[1], patches[2], patches[3]:
        await run_on_k8s_job(ctx)

    call_kwargs = mock_batch.create_namespaced_job.call_args
    job = call_kwargs.args[1]
    container = job.spec.template.spec.containers[0]
    assert "serve" not in container.command


async def test_command_without_serve_unchanged() -> None:
    ctx = make_ctx(command=["python", "-m", "myapp"])
    patches, _, mock_batch = make_k8s_mocks(ctx, succeeded=1)

    with patches[0], patches[1], patches[2], patches[3]:
        await run_on_k8s_job(ctx)

    call_kwargs = mock_batch.create_namespaced_job.call_args
    job = call_kwargs.args[1]
    container = job.spec.template.spec.containers[0]
    assert container.command == ["python", "-m", "myapp"]


async def test_cpu_env_added_to_job_env() -> None:
    ctx = make_ctx(env=[])
    patches, _, mock_batch = make_k8s_mocks(ctx, succeeded=1)

    with patches[0], patches[1], patches[2], patches[3]:
        await run_on_k8s_job(ctx)

    call_kwargs = mock_batch.create_namespaced_job.call_args
    job = call_kwargs.args[1]
    container = job.spec.template.spec.containers[0]
    env_names = [e.name for e in container.env]
    assert "TASK_MANAGER_SPAWN" in env_names


# ---------------------------------------------------------------------------
# run_on_k8s_job — error cases
# ---------------------------------------------------------------------------


async def test_container_not_found_raises() -> None:
    ctx = make_ctx(container_name="main")
    # Put a container with a different name in the deployment
    other_container = MagicMock()
    other_container.name = "sidecar"
    ctx._deployment.spec.template.spec.containers = [other_container]

    patches, _, _ = make_k8s_mocks(ctx, succeeded=1)

    with patches[0], patches[1], patches[2], patches[3]:
        with pytest.raises(TaskRunError, match="Container main not found"):
            await run_on_k8s_job(ctx)


async def test_job_failure_raises() -> None:
    ctx = make_ctx()
    patches, _, mock_batch = make_k8s_mocks(ctx, succeeded=0, failed=3)

    with patches[0], patches[1], patches[2], patches[3]:
        with pytest.raises(TaskRunError, match="K8s task failed"):
            await run_on_k8s_job(ctx)


async def test_job_polls_until_succeeded() -> None:
    """Job status is pending on first poll, succeeded on second."""
    ctx = make_ctx()
    patches, _, mock_batch = make_k8s_mocks(ctx, succeeded=1)

    pending = MagicMock()
    pending.status.succeeded = 0
    pending.status.failed = None

    done = MagicMock()
    done.status.succeeded = 1
    done.status.failed = None

    mock_batch.read_namespaced_job_status.side_effect = [pending, done]

    with patches[0], patches[1], patches[2], patches[3]:
        await run_on_k8s_job(ctx)

    assert mock_batch.read_namespaced_job_status.await_count == 2


# ---------------------------------------------------------------------------
# K8sConfig
# ---------------------------------------------------------------------------


async def test_resources_applied_to_container() -> None:
    resources = K8sResourceRequirements(
        limits={"cpu": "2", "memory": "4Gi"},
        requests={"cpu": "1", "memory": "2Gi"},
    )
    cfg = K8sConfig(
        namespace="workers",
        deployment="fluid-task",
        container="main",
        sleep=0,
        resources=resources,
    )
    ctx = make_ctx(k8s_config=cfg)
    patches, _, mock_batch = make_k8s_mocks(ctx, succeeded=1)

    with patches[0], patches[1], patches[2], patches[3]:
        await run_on_k8s_job(ctx)

    call_kwargs = mock_batch.create_namespaced_job.call_args
    job = call_kwargs.args[1]
    container = job.spec.template.spec.containers[0]
    assert container.resources is not None


async def test_resources_not_set_leaves_container_resources_unchanged() -> None:
    ctx = make_ctx()  # no resources in K8sConfig
    original_resources = ctx._container.resources
    patches, _, mock_batch = make_k8s_mocks(ctx, succeeded=1)

    with patches[0], patches[1], patches[2], patches[3]:
        await run_on_k8s_job(ctx)

    call_kwargs = mock_batch.create_namespaced_job.call_args
    job = call_kwargs.args[1]
    container = job.spec.template.spec.containers[0]
    assert container.resources == original_resources


async def test_k8s_config_defaults() -> None:
    cfg = K8sConfig()
    assert cfg.namespace == "default"
    assert cfg.deployment == "fluid-task"
    assert cfg.container == "main"
    assert cfg.job_ttl == 300


async def test_k8s_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLUID_TASK_CONSUMER_K8S_NAMESPACE", "prod")
    monkeypatch.setenv("FLUID_TASK_CONSUMER_K8S_DEPLOYMENT", "my-consumer")
    monkeypatch.setenv("FLUID_TASK_CONSUMER_K8S_CONTAINER", "worker")
    monkeypatch.setenv("FLUID_TASK_CONSUMER_K8S_JOB_TTL", "600")

    cfg = K8sConfig()
    assert cfg.namespace == "prod"
    assert cfg.deployment == "my-consumer"
    assert cfg.container == "worker"
    assert cfg.job_ttl == 600


# ---------------------------------------------------------------------------
# Circular import guard
# ---------------------------------------------------------------------------


async def test_run_cpu_bound_is_run_on_k8s_job() -> None:
    """When kubernetes_asyncio is installed, run_cpu_bound must be run_on_k8s_job.

    A circular import between k8s_job and models causes run_on_k8s_job to be
    silently set to None (via the try/except ImportError in models.py), which
    means run_cpu_bound stays as run_in_subprocess even inside a k8s cluster.
    """
    pytest.importorskip("kubernetes_asyncio")
    from fluid.scheduler.k8s_job import run_on_k8s_job as k8s_run
    from fluid.scheduler.models import run_on_k8s_job

    assert k8s_run is run_on_k8s_job
