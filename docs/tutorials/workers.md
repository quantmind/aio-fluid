# Workers

Workers are the main building block for asynchronous programming with `aio-fluid`. They are responsible for running tasks and managing their lifecycle.
All workers implemented derive from the base abstract class [Worker][fluid.utils.worker.Worker] where the main method to implement is the [Worker.run][fluid.utils.worker.Worker.run] method.

## Worker Lifecycle

The lifecycle of a worker is managed by the [WorkerState][fluid.utils.worker.WorkerState] class which provides a set of states that a worker can be in. The worker starts in an inital state and than it can be started and stopped.

### Startup

To start a worker one uses the async method [Worker.startup][fluid.utils.worker.Worker.startup] which create the task running the worker. The task will transition the worker from [WorkerState.INIT][fluid.utils.worker.WorkerState.INIT] to the [WorkerState.RUNNING][fluid.utils.worker.WorkerState.RUNNING] state. The worker will then run the [Worker.on_startup][fluid.utils.worker.Worker.on_startup] coroutine method (which by default is a no-op) follow by the main worker coroutine method [Worker.run][fluid.utils.worker.Worker.run] method until it is stopped.

This is a very simple example of a worker that prints a message every second until it is stopped:

```python
--8<-- "./examples/docs/simple_worker.py"
```

### Shutdown

To shut down a worker there are few possibilities.

* Direct call to the async [Worker.shutdown][fluid.utils.worker.Worker.shutdown] method which will trigger the graceful shutdown and wait for the worker to finish its work.
* Call the [Worker.gracefully_stop][fluid.utils.worker.Worker.gracefully_stop] method which will trigger the graceful shutdown. Importantly, this method does not wait for the worker to finish its work, ti simply transition from the [WorkerState.RUNNING][fluid.utils.worker.WorkerState.RUNNING] to [WorkerState.STOPPING][fluid.utils.worker.WorkerState.STOPPING] state. To wait for the worker exit one should call the async [Worker.wait_for_shutdown][fluid.utils.worker.Worker.wait_for_shutdown] method (as in the example above)

### Being a Good Worker

When a worker is asked to stop, it should stop. This is the design, and there is no reason for a worker to keep running once it has been asked to exit. A good worker:

* checks [Worker.is_running][fluid.utils.worker.Worker.is_running] in its main loop and leaves the loop as soon as it returns `False`, as in the example above;
* never blocks for long between those checks: every wait has a timeout short enough for the loop to notice the [WorkerState.STOPPING][fluid.utils.worker.WorkerState.STOPPING] state quickly;
* releases its resources on the way out, in [Worker.run][fluid.utils.worker.Worker.run] itself or in [Worker.on_shutdown][fluid.utils.worker.Worker.on_shutdown].

A worker that does not exit within its stopping grace period (the `stopping_grace_period` argument, or the `FLUID_STOPPING_GRACE_PERIOD` environment variable, 10 seconds by default) is cancelled. It then ends in the [WorkerState.FORCE_STOPPED][fluid.utils.worker.WorkerState.FORCE_STOPPED] state with exit code 1, its [Worker.on_shutdown][fluid.utils.worker.Worker.on_shutdown] is skipped, and a warning is logged. A forced shutdown is the sign of a worker that is not behaving.

The workers shipped with `aio-fluid` are good workers. In particular, when a [QueueConsumer][fluid.utils.worker.QueueConsumer] is cancelled while stopping and waiting on its queue, [QueueConsumer.get_message][fluid.utils.worker.QueueConsumer.get_message] consumes the cancellation and returns `None`, so the consumer leaves its loop and exits cleanly, with [Worker.on_shutdown][fluid.utils.worker.Worker.on_shutdown] called as usual.

## Async Context Manager

[Worker][fluid.utils.worker.Worker] implements the async context manager protocol. Entering the context calls [Worker.startup][fluid.utils.worker.Worker.startup] and exiting it calls [Worker.shutdown][fluid.utils.worker.Worker.shutdown], so the `async with` pattern is the most concise way to manage the full lifecycle:

```python
async with MyWorker() as worker:
    # worker is running here
    ...
# worker is fully shut down here
```

Resources that the worker needs for its entire lifetime can be opened and closed inside [Worker.run][fluid.utils.worker.Worker.run] using normal `async with` statements — no subclassing of lifecycle hooks is required.
