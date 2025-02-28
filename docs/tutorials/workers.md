# Workers

Workers are the main building block for asynchronous programming with `aio-fluid`. They are responsible for running tasks and managing their lifecycle.
All workers implemented derive from the base abstract class [Worker][fluid.utils.worker.Worker] where the main method to implement is the [Worker.run][fluid.utils.worker.Worker.run] method.

## Worker Lifecycle

The lifecycle of a worker is managed by the [WorkerState][fluid.utils.worker.WorkerState] class which provides a set of states that a worker can be in. The worker starts in an inital state and than it can be started and stopped.

### Startup

To start a worker one uses the async method [Worker.startup][fluid.utils.worker.Worker.startup] which create the task running the worker. The task will transition the worker from [WorkerState.INIT][fluid.utils.worker.WorkerState.INIT] to the [WorkerState.RUNNING][fluid.utils.worker.WorkerState.RUNNING] state. The worker will then run the [Worker.on_startup][fluid.utils.worker.Worker.on_startup] coroutine method (which by default is a no-op) follow by the main worker coroutine method [Worker.run][fluid.utils.worker.Worker.run] method until it is stopped.

This is a very simple example of a worker that prints a message every second until it is stopped:

```python
--8<-- "./docs_src/simple_worker.py"
```

### Shutdown

To shut down a worker there are few possibilities.

* Direct call to the async [Worker.shutdown][fluid.utils.worker.Worker.shutdown] method which will trigger the graceful shutdown and wait for the worker to finish its work.
* Call the [Worker.gracefully_stop][fluid.utils.worker.Worker.gracefully_stop] method which will trigger the graceful shutdown. Importantly, this method does not wait for the worker to finish its work, ti simply transition from the [WorkerState.RUNNING][fluid.utils.worker.WorkerState.RUNNING] to [WorkerState.STOPPING][fluid.utils.worker.WorkerState.STOPPING] state. To wait for the worker exit one should call the async [Worker.wait_for_shutdown][fluid.utils.worker.Worker.wait_for_shutdown] method (as in the example above)
