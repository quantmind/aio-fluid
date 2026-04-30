from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient
from slugify import slugify

from .common import cpu_env
from .errors import TaskAbortedError, TaskRunError

if TYPE_CHECKING:
    from .models import K8sConfig, TaskRun


def get_job_name(ctx: TaskRun) -> str:
    short_task_id = ctx.id[:7]
    job_name = slugify(f"task-{ctx.name}-{short_task_id}")[:63]
    return job_name


async def run_on_k8s_job(ctx: TaskRun) -> None:
    """Run a task on a k8s job

    This is available when running inside a Kubernetes cluster.
    Only task consumer/scheduler with command line client can use this
    """
    task = ctx.task
    job_name = get_job_name(ctx)
    # load k8s config from within the cluster
    config.load_incluster_config()
    k8s_config = task.get_k8s_config()
    async with ApiClient() as api:
        v1 = client.AppsV1Api(api)
        # get the targeted deployment from the targeted namespace
        tasks = await v1.read_namespaced_deployment(
            k8s_config.deployment,
            k8s_config.namespace,
        )
        # pod template
        pod_template = k8s_job_pod_template(
            ctx,
            tasks.spec.template,
            k8s_config,
        )
        batch = client.BatchV1Api(api)
        job = client.V1Job(
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                ttl_seconds_after_finished=k8s_config.job_ttl,
                backoff_limit=0,
                template=pod_template,
            ),
        )
        response = await batch.create_namespaced_job(k8s_config.namespace, job)
        ctx.logger.info(f"Job created. status={response.status}")
        while True:
            job_status = await batch.read_namespaced_job_status(
                name=job_name,
                namespace=k8s_config.namespace,
            )
            if job_status.status.succeeded:
                ctx.logger.info(f"status={job_status.status}")
                if reason := await ctx.task_manager.broker.get_task_aborted(ctx.id):
                    raise TaskAbortedError(reason)
                break
            if job_status.status.failed is not None:
                raise TaskRunError(f"K8s task failed status {job_status.status}")
            await asyncio.sleep(k8s_config.sleep)


def k8s_job_pod_template(
    ctx: TaskRun, pod_template: client.V1PodTemplateSpec, k8s_config: K8sConfig
) -> client.V1PodTemplateSpec:
    """Modify the pod template for the k8s job."""
    # pod spec for the job is based on the deployment's pod spec,
    # but with a different command and args
    pod_spec = pod_template.spec
    # get the targeted container from the pod spec
    container = next(
        (c for c in pod_spec.containers if c.name == k8s_config.container),
        None,
    )
    if container is None:
        raise TaskRunError(f"Container {k8s_config.container} not found")
    command = list(container.command or [])
    if command and command[-1] == "serve":
        command.pop()
    container.command = command
    container.args = [
        "exec",
        ctx.name,
        "--log",
        "--run-id",
        ctx.id,
        "--params",
        ctx.params.model_dump_json(),
    ]
    # resources
    if resources := k8s_config.resources:
        container.resources = client.V1ResourceRequirements(**resources)
    # env vars for the task context
    env = list(container.env or [])
    for name, value in cpu_env().items():
        env.append(client.V1EnvVar(name=name, value=value))
    container.env = env
    container.liveness_probe = None  # type: ignore[assignment]
    container.readiness_probe = None  # type: ignore[assignment]
    pod_spec.containers = [container]
    pod_spec.restart_policy = "Never"
    return pod_template
