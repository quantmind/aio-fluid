# K8s Jobs

When the [TaskConsumer][fluid.scheduler.TaskConsumer] runs inside a Kubernetes cluster, [CPU bound tasks][fluid.scheduler.task] can be dispatched as [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/) instead of local subprocesses.
This offloads heavy computation to dedicated pods, keeping the consumer event loop free.

## How it works

The switch is automatic. When `KUBERNETES_SERVICE_HOST` is set (which Kubernetes injects into every pod) and the `k8s` extra is installed, any task declared with `cpu_bound=True` will spawn a Kubernetes Job instead of a subprocess. No code change is required in the task itself.

The Job reuses the same container image and command as the task consumer deployment, running:

```
<consumer-command> exec <task-name> --log --run-id <id> --params <json>
```

The consumer waits for the Job to complete and raises an error if it fails.

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
    # heavy CPU work here — runs in a k8s Job when inside a cluster
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
