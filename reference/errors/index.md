# Errors

`aio-fluid` defines two error hierarchies: one for general utilities and one for the task scheduler.

## Utility errors

```python
from fluid.utils.errors import FluidError, FluidValueError, ValidationError, WorkerStartError
```

## fluid.utils.errors.FluidError

Bases: `Exception`

Base class for all Fluid Trading errors.

## fluid.utils.errors.FluidValueError

Bases: `ValueError`, `FluidError`

Quant Value Error

## fluid.utils.errors.ValidationError

```python
ValidationError(
    field, msg="", field_type="string", value=None
)
```

Bases: `FluidValueError`

Validation Error

Source code in `fluid/utils/errors.py`

```python
def __init__(
    self, field: str, msg: str = "", field_type: str = "string", value: Any = None
):
    self.field = field
    self.field_type = field_type
    self.msg = msg or "validation error"
    self.value = value
```

### field

```python
field = field
```

### field_type

```python
field_type = field_type
```

### msg

```python
msg = msg or 'validation error'
```

### value

```python
value = value
```

## fluid.utils.errors.WorkerStartError

Bases: `FluidError`

Worker start error

## fluid.utils.errors.FlamegraphError

Bases: `FluidError`

Raised when flamegraph generation fails.

## Task scheduler errors

```python
from fluid.scheduler.errors import TaskError, UnknownTaskError, DisabledTaskError
```

## fluid.scheduler.errors.TaskError

Bases: `RuntimeError`

Base class for all task scheduler errors.

## fluid.scheduler.errors.UnknownTaskError

Bases: `TaskError`

Raised when a task name is not registered in the task registry.

## fluid.scheduler.errors.DisabledTaskError

Bases: `TaskError`

Raised when attempting to queue or run a disabled task.

## fluid.scheduler.errors.TaskParamsError

```python
TaskParamsError(task_run, message)
```

Bases: `TaskError`

Raised when task run parameters fail validation when consumed from the broker.

It carries the task run, created with unvalidated params, so the consumer can mark it as failed and log the error.

Source code in `fluid/scheduler/errors.py`

```python
def __init__(self, task_run: TaskRun, message: str) -> None:
    super().__init__(message)
    self.task_run = task_run
```

### task_run

```python
task_run = task_run
```

## fluid.scheduler.errors.TaskRunError

Bases: `TaskError`

Raised when a task run fails during execution.

This is an internal error used to signal a failure during task execution, and is not intended to be raised by user code.

## fluid.scheduler.errors.TaskAbortedError

Bases: `TaskError`

Raised when a task run is aborted before completion.

If a task needs to abort itself it can raise this error, which will be caught by the consumer and treated as a soft-failure and therefore logged as info and not trigger any retry policy if configured.

## fluid.scheduler.errors.TaskDecoratorError

Bases: `TaskError`

Raised when a task is incorrectly decorated or configured.

## fluid.scheduler.errors.CpuBoundEntryPointError

Bases: `TaskError`

Raised when a cpu bound task cannot be executed by the entry point.

Cpu bound tasks run in a separate process, started by the `exec` command of the task manager command line client. An application which does not expose one cannot execute them.
