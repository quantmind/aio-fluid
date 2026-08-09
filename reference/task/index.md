# Task

A Task defines the implementation of a given operation, the inputs required and the scheduling metadata. Usually, a Task is not created directly, but rather through the use of the @task decorator.

## Example

A task function is decorated via the @task decorator and must accept the TaskRun object as its first and only argument.

```python
from fluid.scheduler import task, TaskRun

@task
async def hello(ctx: TaskRun) -> None:
    print("Hello, world!")
```

For retry configuration (`retry`, `rate_limit_retry`) see [Task Retry](https://fluid.quantmind.com/reference/task_retry/index.md).

## fluid.scheduler.task

```python
task(executor: TaskExecutor) -> Task
```

```python
task(
    *,
    name: str | None = None,
    schedule: Scheduler | None = None,
    short_description: str | None = None,
    description: str | None = None,
    randomize: RandomizeType | None = None,
    max_concurrency: int | None = None,
    priority: TaskPriority | None = None,
    cpu_bound: bool | None = None,
    k8s_config: K8sConfig | None = None,
    timeout_seconds: int | None = None,
    tags: Sequence[str] | None = None,
    retry: RetryPolicy | None = None,
    rate_limit_retry: RetryPolicy | None = None,
    env: dict[str, str] | None = None
) -> TaskConstructor
```

```python
task(
    executor=None,
    *,
    name=None,
    schedule=None,
    short_description=None,
    description=None,
    randomize=None,
    max_concurrency=None,
    priority=None,
    cpu_bound=None,
    k8s_config=None,
    timeout_seconds=None,
    tags=None,
    retry=None,
    rate_limit_retry=None,
    env=None
)
```

Decorator to create a Task from a function and optional parameters.

This decorator can be used in two ways:

- As a simple decorator of the executor function
- As a function with keyword arguments for greater control over the task configuration

| PARAMETER           | DESCRIPTION                                                                                                                       |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `executor`          | The executor function for the task **TYPE:** \`TaskExecutor                                                                       |
| `name`              | The name of the task. If None, the name will be derived from the executor function **TYPE:** \`str                                |
| `schedule`          | The schedule for the tas. If None, the task will not be scheduled **TYPE:** \`Scheduler                                           |
| `short_description` | A short description of the task. If not provided it will be extracted from the task function docstring first line **TYPE:** \`str |
| `description`       | A detailed description of the task. If not provided it will be extracted from the task function docstring **TYPE:** \`str         |
| `randomize`         | Randomization settings for the task **TYPE:** \`RandomizeType                                                                     |
| `max_concurrency`   | The maximum number of concurrent executions of the task **TYPE:** \`int                                                           |
| `priority`          | The priority of the task such as high, medium, low **TYPE:** \`TaskPriority                                                       |
| `cpu_bound`         | Whether the task is CPU bound **TYPE:** \`bool                                                                                    |
| `k8s_config`        | Kubernetes configuration - None means use the default configuration **TYPE:** \`K8sConfig                                         |
| `timeout_seconds`   | Task timeout in seconds - how long the task can run before being aborted **TYPE:** \`int                                          |
| `tags`              | Task tags - used for categorization and filtering of tasks **TYPE:** \`Sequence[str]                                              |
| `retry`             | Retry policy for execution failures **TYPE:** \`RetryPolicy                                                                       |
| `rate_limit_retry`  | Retry policy when the task is rate limited by max_concurrency **TYPE:** \`RetryPolicy                                             |
| `env`               | Extra environment variables injected into the subprocess or k8s job **TYPE:** \`dict[str, str]                                    |

Source code in `fluid/scheduler/models.py`

```python
def task(
    executor: Annotated[
        TaskExecutor | None,
        Doc("The executor function for the task"),
    ] = None,
    *,
    name: Annotated[
        str | None,
        Doc(
            (
                "The name of the task. If None, the name will be derived "
                "from the executor function"
            )
        ),
    ] = None,
    schedule: Annotated[
        Scheduler | None,
        Doc("The schedule for the tas. If None, the task will not be scheduled"),
    ] = None,
    short_description: Annotated[
        str | None,
        Doc(
            (
                "A short description of the task. "
                "If not provided it will be extracted from the task function docstring "
                "first line"
            )
        ),
    ] = None,
    description: Annotated[
        str | None,
        Doc(
            (
                "A detailed description of the task. "
                "If not provided it will be extracted from the task function docstring"
            )
        ),
    ] = None,
    randomize: Annotated[
        RandomizeType | None,
        Doc("Randomization settings for the task"),
    ] = None,
    max_concurrency: Annotated[
        int | None,
        Doc(("The maximum number of concurrent executions of the task")),
    ] = None,
    priority: Annotated[
        TaskPriority | None,
        Doc("The priority of the task such as high, medium, low"),
    ] = None,
    cpu_bound: Annotated[
        bool | None,
        Doc("Whether the task is CPU bound"),
    ] = None,
    k8s_config: Annotated[
        K8sConfig | None,
        Doc("Kubernetes configuration - None means use the default configuration"),
    ] = None,
    timeout_seconds: Annotated[
        int | None,
        Doc("Task timeout in seconds - how long the task can run before being aborted"),
    ] = None,
    tags: Annotated[
        Sequence[str] | None,
        Doc("Task tags - used for categorization and filtering of tasks"),
    ] = None,
    retry: Annotated[
        RetryPolicy | None,
        Doc("Retry policy for execution failures"),
    ] = None,
    rate_limit_retry: Annotated[
        RetryPolicy | None,
        Doc("Retry policy when the task is rate limited by max_concurrency"),
    ] = None,
    env: Annotated[
        dict[str, str] | None,
        Doc("Extra environment variables injected into the subprocess or k8s job"),
    ] = None,
) -> Task | TaskConstructor:
    """Decorator to create a [Task][fluid.scheduler.Task] from a function
    and optional parameters.

    This decorator can be used in two ways:

    - As a simple decorator of the executor function
    - As a function with keyword arguments for greater control
        over the task configuration
    """
    kwargs = compact_dict(
        name=name,
        schedule=schedule,
        short_description=short_description,
        description=description,
        randomize=randomize,
        max_concurrency=max_concurrency,
        priority=priority,
        cpu_bound=cpu_bound,
        k8s_config=k8s_config,
        timeout_seconds=timeout_seconds,
        tags=frozenset(tags) if tags is not None else None,
        retry=retry,
        rate_limit_retry=rate_limit_retry,
        env=env,
    )
    if kwargs and executor:
        raise TaskDecoratorError("cannot use positional parameters")
    elif kwargs:
        return TaskConstructor(**kwargs)
    elif not executor:
        raise TaskDecoratorError("this is a decorator cannot be invoked in this way")
    else:
        return TaskConstructor()(executor)
```

## fluid.scheduler.Task

Bases: `NamedTuple`, `Generic[TP]`

A Task configuration.

This is not created directly, but rather through the use of the @task decorator.

Executes any time it is invoked

### name

```python
name
```

Task name - unique identifier

### executor

```python
executor
```

Task executor function

### params_model

```python
params_model
```

Pydantic model for task parameters

### logger

```python
logger
```

Task logger

### module

```python
module = ''
```

Task python module

### short_description

```python
short_description = ''
```

Short task description - one line

### description

```python
description = ''
```

Task description - obtained from the executor docstring if not provided

### schedule

```python
schedule = None
```

Task schedule - None means the task is not scheduled

### randomize

```python
randomize = None
```

Randomize function for task schedule

### max_concurrency

```python
max_concurrency = 0
```

how many tasks can be run concurrently - 0 means no limit

### timeout_seconds

```python
timeout_seconds = 60
```

Task timeout in seconds - how long the task can run before being aborted

### priority

```python
priority = TaskPriority.medium
```

Task priority - high, medium, low

### k8s_config

```python
k8s_config = None
```

Kubernetes configuration for tasks run on Kubernetes cluster.

### tags

```python
tags = frozenset()
```

Task tags - used for categorization and filtering of tasks

### retry

```python
retry = None
```

Retry policy for general execution failures.

### rate_limit_retry

```python
rate_limit_retry = None
```

Retry policy when the executor raises `RateLimitError`.

### env

```python
env = {}
```

Extra environment variables injected into the subprocess or k8s job.

### cpu_bound

```python
cpu_bound
```

True if the task is CPU bound

### get_k8s_config

```python
get_k8s_config()
```

Get Kubernetes configuration for this task

Source code in `fluid/scheduler/models.py`

```python
def get_k8s_config(self) -> K8sConfig:
    """Get Kubernetes configuration for this task"""
    return self.k8s_config or K8sConfig()
```

### info

```python
info(**params)
```

Return task info object

Source code in `fluid/scheduler/models.py`

```python
def info(self, **params: Any) -> TaskInfo:
    """Return task info object"""
    params.update(
        name=self.name,
        description=self.description,
        module=self.module,
        priority=self.priority,
        schedule=str(self.schedule) if self.schedule else None,
        tags=self.tags,
    )
    return TaskInfo(**compact_dict(params))
```

## fluid.scheduler.TaskPriority

Bases: `StrEnum`

Priority level for task execution ordering.

### high

```python
high = enum.auto()
```

Execute before medium and low priority tasks.

### medium

```python
medium = enum.auto()
```

Default priority level.

### low

```python
low = enum.auto()
```

Execute after high and medium priority tasks.

## fluid.scheduler.TaskState

Bases: `StrEnum`

Lifecycle state of a task run.

### init

```python
init = enum.auto()
```

Task has been created but not yet queued.

### queued

```python
queued = enum.auto()
```

Task is waiting in the queue to be picked up by a worker.

### running

```python
running = enum.auto()
```

Task is currently being executed.

### success

```python
success = enum.auto()
```

Task completed successfully.

### failure

```python
failure = enum.auto()
```

Task raised an exception during execution.

### aborted

```python
aborted = enum.auto()
```

Task was cancelled before completion.

### rate_limited

```python
rate_limited = enum.auto()
```

Task execution was deferred due to rate limiting.

### interrupted

```python
interrupted = enum.auto()
```

Task was interrupted by a worker shutdown before it could complete.

### is_failure

```python
is_failure
```

Return True if this state is a failure state

### is_done

```python
is_done
```

Return True if this state is a finished state

## fluid.scheduler.K8sConfig

Bases: `BaseModel`

Kubernetes configuration for tasks run on Kubernetes cluster. This configuration is used by the task consumer to run tasks on Kubernetes Jobs.

```python
from fluid.scheduler import K8sConfig
```

This is used when the task consumer runs inside a Kubernetes cluster and the task is marked as CPU bound.

Fields:

- `namespace` (`str`)
- `deployment` (`str`)
- `container` (`str`)
- `resources` (`K8sResourceRequirements | None`)
- `job_ttl` (`int`)
- `sleep` (`float`)

### namespace

```python
namespace
```

Kubernetes namespace where the task consumer deployment run

### deployment

```python
deployment
```

Kubernetes deployment of the task consumer

### container

```python
container
```

Kubernetes container

### resources

```python
resources = None
```

Kubernetes resource limits and requests for the container

### job_ttl

```python
job_ttl
```

Time to live for k8s Job after completion

### sleep

```python
sleep
```

Amount to async sleep while waiting for completion of k8s Job

## fluid.scheduler.K8sResourceRequirements

Bases: `TypedDict`

CPU and memory limits/requests for a Kubernetes container.

### limits

```python
limits
```

### requests

```python
requests
```

## fluid.scheduler.is_in_cpu_process

```python
is_in_cpu_process()
```

Check if the current process is a CPU process.

A CPU process is a process that is spawned by the task manager to run a cpu-bound task.

It is identified by the environment variable `TASK_MANAGER_SPAWN` being set to "true".

Source code in `fluid/scheduler/common.py`

```python
def is_in_cpu_process() -> bool:
    """Check if the current process is a CPU process.

    A CPU process is a process that is spawned by the task manager to run
    a cpu-bound task.

    It is identified by the environment variable `TASK_MANAGER_SPAWN`
    being set to "true".
    """
    return os.getenv("TASK_MANAGER_SPAWN") == "true"
```
