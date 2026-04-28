# Errors

`aio-fluid` defines two error hierarchies: one for general utilities and one for the task scheduler.

## Utility errors

```python
from fluid.utils.errors import FluidError, FluidValueError, ValidationError, WorkerStartError
```

::: fluid.utils.errors.FluidError

::: fluid.utils.errors.FluidValueError

::: fluid.utils.errors.ValidationError

::: fluid.utils.errors.WorkerStartError

::: fluid.utils.errors.FlamegraphError

## Task scheduler errors

```python
from fluid.scheduler.errors import TaskError, UnknownTaskError, DisabledTaskError
```

::: fluid.scheduler.errors.TaskError

::: fluid.scheduler.errors.UnknownTaskError

::: fluid.scheduler.errors.DisabledTaskError

::: fluid.scheduler.errors.TaskRunError

::: fluid.scheduler.errors.TaskAbortedError

::: fluid.scheduler.errors.TaskDecoratorError
