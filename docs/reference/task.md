# Task

A [Task][fluid.scheduler.Task] defines the implementation of a given operation, the inputs required and the scheduling metadata.
Usually, a [Task][fluid.scheduler.Task] is not created directly, but rather through the use of the [@task][fluid.scheduler.task] decorator.

## Example

A task function is decorated via the [@task][fluid.scheduler.task] decorator and must accept the [TaskRun][fluid.scheduler.TaskRun] object as its first and only argument.

```python
from fluid.scheduler import task, TaskRun

@task
async def hello(ctx: TaskRun) -> None:
    print("Hello, world!")
```


For retry configuration (`retry`, `rate_limit_retry`) see [Task Retry](task_retry.md).

::: fluid.scheduler.task

::: fluid.scheduler.Task

::: fluid.scheduler.TaskPriority

::: fluid.scheduler.TaskState

::: fluid.scheduler.K8sConfig

::: fluid.scheduler.is_in_cpu_process
