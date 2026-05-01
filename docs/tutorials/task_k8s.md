# K8s Jobs

When the [TaskConsumer][fluid.scheduler.TaskConsumer] runs inside a Kubernetes cluster, [CPU bound tasks](/tutorials/task_queue/#cpu-bound-tasks) can be dispatched as [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/) instead of local subprocesses.
This offloads heavy computation to dedicated pods and keeps the consumer event loop free.

## How it works

The switch is automatic. When `KUBERNETES_SERVICE_HOST` is set (which Kubernetes injects into every pod) and the `k8s` extra is installed, any task declared with `cpu_bound=True` will spawn a Kubernetes Job instead of a subprocess. No code change is required in the task itself.

The Job pod template is derived from the **task consumer deployment**. The implementation reads the deployment, locates the target container, and builds a Job spec from it. This means the Job inherits most of the container's configuration from the deployment — image, image pull policy, volume mounts, security context, and everything else — while only overriding the fields necessary to run the task.


**Inherited from the deployment container (unchanged):**

- Container image and image pull policy
- Volume mounts (and pod-level volumes)
- Environment variables (the task's env vars are appended, never replaced)
- Security context
- Everything else not listed below

**Overridden or cleared:**

| Field | Value in the Job |
|---|---|
| `command` | Same as the deployment, but any trailing `serve` token is removed |
| `args` | `exec <task-name> --log --run-id <id> --params <json>` |
| `env` | Inherited from the deployment, then `TASK_MANAGER_SPAWN=true` appended, then any task-level `env` vars appended (see [Injecting environment variables](#injecting-environment-variables)) |
| `resources` | Inherited from the deployment unless overridden via [`K8sConfig.resources`](#configuration) |
| `liveness_probe` | Cleared — probes are not meaningful for Job pods and would prematurely kill long-running tasks |
| `readiness_probe` | Cleared |

**Other containers** (sidecars) from the deployment are dropped — only the target container runs in the Job pod. **Pod-level init containers** and **volumes** are preserved, so any setup performed at pod startup (e.g. installing TLS certificates) is reproduced in the Job pod.

`TASK_MANAGER_SPAWN=true` signals to the process inside the Job that it is a CPU-bound worker rather than a long-lived consumer.

The Job is created in the same namespace as the consumer with:

- `backoff_limit: 0` — a failed pod is never retried; the error is propagated back to the task consumer instead
- `ttlSecondsAfterFinished` — set from [`K8sConfig.job_ttl`](#configuration), the Job and its pods are cleaned up automatically after completion (default 300 s)
- `restartPolicy: Never` on the pod template

The job name is derived from the task name and the first 7 characters of the run ID, slugified and capped at 63 characters to comply with Kubernetes DNS label requirements:

```
task-<slugified-task-name>-<short-run-id>
```

Once the Job is created, the consumer polls its status every [`K8sConfig.sleep`](#configuration) seconds until it either succeeds or fails.

## Installation

It requires both the `cli` and `k8s` extras:

```bash
pip install aio-fluid[cli,k8s]
```

## Defining a CPU bound task

```python
from fluid.scheduler import task, TaskRun

@task(cpu_bound=True)
async def heavy_calculation(ctx: TaskRun) -> None:
    # heavy CPU work here — runs in a k8s Job when inside a cluster,
    # or in a local subprocess when running outside one
    ...
```

## Configuration

K8s behaviour can be tuned per-task via the `k8s_config` argument, which accepts a [K8sConfig][fluid.scheduler.K8sConfig] object:

```python
from fluid.scheduler import task, TaskRun, K8sConfig

@task(
    cpu_bound=True,
    k8s_config=K8sConfig(
        namespace="workers",      # namespace where the Job is created
        deployment="fluid-task",  # deployment to copy the container spec from
        container="main",         # container name inside the deployment
        job_ttl=600,              # seconds to keep the Job after completion (default 300)
        sleep=2.0,                # polling interval while waiting for the Job (default 2.0)
        resources={               # override the container's resource spec (default: inherited from deployment)
            "limits": {"cpu": "2", "memory": "4Gi"},
            "requests": {"cpu": "1", "memory": "2Gi"},
        },
    ),
)
async def heavy_calculation(ctx: TaskRun) -> None:
    ...
```

### K8sConfig fields

| Field | Type | Default | Description |
|---|---|---|---|
| `namespace` | `str` | `FLUID_TASK_CONSUMER_K8S_NAMESPACE` or `"default"` | Kubernetes namespace where the Job is created |
| `deployment` | `str` | `FLUID_TASK_CONSUMER_K8S_DEPLOYMENT` or `"fluid-task"` | Deployment to read the container spec from |
| `container` | `str` | `FLUID_TASK_CONSUMER_K8S_CONTAINER` or `"main"` | Container name within the deployment |
| `resources` | `K8sResourceRequirements \| None` | `None` | Resource limits/requests for the Job container. If `None`, the deployment's existing resource spec is used unchanged |
| `job_ttl` | `int` | `FLUID_TASK_CONSUMER_K8S_JOB_TTL` or `300` | Seconds to retain the Job after completion before automatic cleanup |
| `sleep` | `float` | `FLUID_TASK_CONSUMER_K8S_SLEEP` or `2.0` | Polling interval in seconds while waiting for the Job to finish |

All `K8sConfig` fields have defaults drawn from environment variables, so a minimal deployment only needs to set those variables rather than hard-coding values per task.

If `k8s_config` is omitted entirely, a [K8sConfig][fluid.scheduler.K8sConfig] instance with all defaults is used.

### Resource overrides

The `resources` field accepts a [K8sResourceRequirements][fluid.scheduler.K8sResourceRequirements] dict with optional `limits` and `requests` keys:

```python
resources={
    "limits":   {"cpu": "4",   "memory": "8Gi"},
    "requests": {"cpu": "500m", "memory": "1Gi"},
}
```

When not provided (the default), the Job container inherits the resource spec from the deployment container unchanged. This is useful for tasks that need more CPU or memory than the consumer pod is allocated.

## Injecting environment variables

Extra environment variables can be injected into the Job (or subprocess, when running outside a cluster) using the `env` argument on the [`@task`][fluid.scheduler.task] decorator:

```python
from fluid.scheduler import task, TaskRun

@task(
    cpu_bound=True,
    env={"MODEL_PATH": "/mnt/models/v2", "LOG_LEVEL": "DEBUG"},
)
async def heavy_calculation(ctx: TaskRun) -> None:
    import os
    model_path = os.environ["MODEL_PATH"]
    ...
```

These variables are appended to the environment after the deployment's existing env vars and `TASK_MANAGER_SPAWN=true`, so they can override anything set in the deployment if needed.

For subprocess execution (outside a cluster), they are merged into the spawned process's environment the same way, making task definitions portable across both runtimes without any conditional logic.

## Required RBAC permissions

The pod running the [TaskConsumer][fluid.scheduler.TaskConsumer] or
the [TaskScheduler][fluid.scheduler.TaskScheduler] needs permission to read
the deployment and create/read jobs.

Assuming the consumer/scheduler runs in the `workers` namespace, a minimal `Role` and `RoleBinding` can be defined as follows:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: execute-jobs-role
  namespace: workers
rules:
  - apiGroups:
      - apps
    resources:
      - deployments
    verbs:
      - get
      - list
      - watch
  - apiGroups:
      - batch
    resources:
      - jobs
      - cronjobs
      - jobs/status
    verbs:
      - create
      - get
      - list
      - watch
      - delete
      - patch
      - update
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: tasks-consumer-sa
  namespace: workers
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: tasks-consumer-rb
  namespace: workers
subjects:
  - kind: ServiceAccount
    name: tasks-consumer-sa
roleRef:
  kind: Role
  name: execute-jobs-role
  apiGroup: rbac.authorization.k8s.io
```

The `tasks-consumer-sa` ServiceAccount should be used by the consumer/scheduler deployment.
