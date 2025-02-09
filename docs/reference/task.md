# Task

A Task defines the implementation of a given operation, the inputs required and the scheduling metadata.
Usually, a Task is not created directly, but rather through the use of the [@task][fluid.scheduler.task] decorator.

## Example

A task function is decorated vya the [@task][fluid.scheduler.task] decorator and must accept the [TaskRun][fluid.scheduler.TaskRun] object as its first and only argument.

```python
from fluid.scheduler import task, TaskRun

@task
def hello(ctx: TaskRun) -> None:
    print("Hello, world!")
```


::: fluid.scheduler.task

::: fluid.scheduler.Task
