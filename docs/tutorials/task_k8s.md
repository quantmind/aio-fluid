# K8s Jobs

When the [TaskConsumer][fluid.scheduler.TaskConsumer] runs inside a Kubernetes cluster, [CPU bound tasks][fluid.scheduler.task] can be dispatched as [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/) instead of local subprocesses.
This offloads heavy computation to dedicated pods and keeps the consumer event loop free.

## How it works

The switch is automatic. When `KUBERNETES_SERVICE_HOST` is set (which Kubernetes injects into every pod) and the `k8s` extra is installed, any task declared with `cpu_bound=True` will spawn a Kubernetes Job instead of a subprocess. No code change is required in the task itself.

The Job reuses the **full container spec** from the task consumer deployment (image, resource limits, volume mounts, security context, image pull policy, and everything else) as well as the **pod-level init containers** and **volumes**, so any setup performed by init containers (e.g. installing TLS certificates) is reproduced in the Job pod. Only three fields on the main container are overridden:

| Field | Value |
|---|---|
| `command` | same as the deployment container, with any trailing `serve` token removed |
| `args` | `exec <task-name> --log --run-id <id> --params <json>` |
| `env` | same as the deployment container, with `TASK_MANAGER_SPAWN=true` appended |
| `liveness_probe` | cleared — probes are not meaningful for Job pods and could prematurely kill a long-running task |
| `readiness_probe` | cleared |

The `TASK_MANAGER_SPAWN=true` environment variable signals to the process running inside the Job that it is executing as a CPU-bound worker rather than a long-lived consumer.

The Job is created in the same namespace as the consumer with:

- `backoff_limit: 0`: a failed pod is never retried; the error is propagated back to the task consumer instead
- `ttlSecondsAfterFinished`: set from `K8sConfig.job_ttl`, the Job and its pods are cleaned up automatically after completion (default 300 s)
- `restartPolicy: Never` on the pod template

The job name is derived from the task name and the first 7 characters of the run ID, slugified and capped at 63 characters to comply with Kubernetes DNS label requirements:

```
task-<slugified-task-name>-<short-run-id>
```

Once the Job is created, the consumer polls its status every `sleep` seconds until it either succeeds or fails.

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
    # heavy CPU work here, runs in a k8s Job when inside a cluster
    ...
```

## Configuration

K8s behaviour can be tuned per-task via the `k8s_config` argument:

```python
from fluid.scheduler import task, TaskRun, K8sConfig

@task(
    cpu_bound=True,
    k8s_config=K8sConfig(
        namespace="workers",     # namespace where the Job is created
        deployment="fluid-task", # deployment to copy the container spec from
        container="main",        # container name inside the deployment
        job_ttl=600,             # seconds to keep the Job after completion
        sleep=2.0,               # polling interval while waiting for the Job
    ),
)
async def heavy_calculation(ctx: TaskRun) -> None:
    ...
```

If `k8s_config` is omitted, the following environment variables are used:

| Variable | Default | Description |
|---|---|---|
| `FLUID_TASK_CONSUMER_K8S_NAMESPACE` | `default` | Kubernetes namespace |
| `FLUID_TASK_CONSUMER_K8S_DEPLOYMENT` | `fluid-task` | Deployment name |
| `FLUID_TASK_CONSUMER_K8S_CONTAINER` | `main` | Container name |
| `FLUID_TASK_CONSUMER_K8S_JOB_TTL` | `300` | Job TTL in seconds |

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
