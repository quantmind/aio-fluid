# Kernel

Run an external program from async code without blocking the event loop. It is
what runs [CPU bound tasks](../tutorials/tasks.md#cpu-bound-tasks) in a
subprocess, and it is available for running any command line tool from a task.
See [Run a Command Line Tool](../tutorials/task_command.md) for a walkthrough.

```python
from fluid.utils import kernel

code = await kernel.run("gzip", "--keep", "data.csv")
```

::: fluid.utils.kernel.run

::: fluid.utils.kernel.run_python

::: fluid.utils.kernel.terminate

::: fluid.utils.kernel.KernelCallback

::: fluid.utils.kernel.CollectBytes

::: fluid.utils.kernel.EncodedLog

::: fluid.utils.kernel.Utf8Decoder

::: fluid.utils.kernel.TERMINATE_GRACE_PERIOD
