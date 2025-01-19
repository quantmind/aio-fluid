# Task

A Task defines the implementation given operation, the inputs required and the scheduling metadata.
Usually, a Task is not created directly, but rather through the use of the `@task` decorator.

## Example

```python
from fluid.scheduler import task, TaskRun

@task
def hello(ctx: TaskRun) -> None:
    print("Hello, world!")
```


::: fluid.scheduler.task

::: fluid.scheduler.Task
