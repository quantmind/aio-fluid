from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import kubernetes.client as k8s
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient
from slugify import slugify

from .errors import TaskRunError

if TYPE_CHECKING:
    from .models import TaskRun


async def run_on_k8s_job(ctx: TaskRun) -> None:
    task = ctx.task
    job_name = slugify(f"task-{ctx.name}-{ctx.id}")[:63]
    config.load_incluster_config()
    async with ApiClient() as api:
        v1 = client.AppsV1Api(api)
        tasks = await v1.read_namespaced_deployment(
            task.k8s_config.deployment, task.k8s_config.namespace
        )
        container = tasks.spec.template.spec.containers[0]
        batch = client.BatchV1Api(api)
        env = container.env or []
        env.append(k8s.V1EnvVar(name="TASK_MANAGER_SPAWN", value="true"))
        job = k8s.V1Job(
            metadata=k8s.V1ObjectMeta(name=job_name),
            spec=k8s.V1JobSpec(
                ttl_seconds_after_finished=300,
                template=k8s.V1PodTemplateSpec(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name=task.k8s_config.container,
                                image=container.image,
                                command=["python", "main.py"],
                                args=[
                                    "tasks",
                                    "execute",
                                    ctx.name,
                                    "--run-id",
                                    ctx.id,
                                    "--params",
                                    ctx.params.model_dump_json(),
                                ],
                                env=env,
                            )
                        ],
                        restart_policy="Never",
                    )
                ),
            ),
        )
        response = await batch.create_namespaced_job(task.k8s_config.namespace, job)
        ctx.logger.info(f"Job created. status={response.status}")
        while True:
            job_status = await batch.read_namespaced_job_status(
                name=job_name, namespace=task.k8s_config.namespace
            )
            if job_status.status.succeeded:
                ctx.logger.info(f"status={job_status.status}")
                break
            if job_status.status.failed is not None:
                raise TaskRunError(f"K8s task failed status {job_status.status}")
            await asyncio.sleep(task.k8s_config.sleep)
