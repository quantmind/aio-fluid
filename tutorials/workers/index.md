# Workers

Workers are the main building block for asynchronous programming with `aio-fluid`. They are responsible for running tasks and managing their lifecycle. All workers implemented derive from the base abstract class Worker where the main method to implement is the Worker.run method.

## Worker Lifecycle

The lifecycle of a worker is managed by the WorkerState class which provides a set of states that a worker can be in. The worker starts in an inital state and than it can be started and stopped.

### Startup

To start a worker one uses the async method Worker.startup which create the task running the worker. The task will transition the worker from WorkerState.INIT to the WorkerState.RUNNING state. The worker will then run the Worker.on_startup coroutine method (which by default is a no-op) follow by the main worker coroutine method Worker.run method until it is stopped.

This is a very simple example of a worker that prints a message every second until it is stopped:

```python
import asyncio

from fluid.utils.worker import Worker


class SimpleWorker(Worker):
    async def run(self):
        while self.is_running():
            self.print_message()
            await asyncio.sleep(1)

    def print_message(self):
        print(f"Hello from {self.worker_name} in state {self.worker_state}")


async def main():
    worker = SimpleWorker()
    worker.print_message()
    await worker.startup()
    asyncio.get_event_loop().call_later(5, worker.gracefully_stop)
    await worker.wait_for_shutdown()
    worker.print_message()


if __name__ == "__main__":
    asyncio.run(main())
```

### Shutdown

To shut down a worker there are few possibilities.

- Direct call to the async Worker.shutdown method which will trigger the graceful shutdown and wait for the worker to finish its work.
- Call the Worker.gracefully_stop method which will trigger the graceful shutdown. Importantly, this method does not wait for the worker to finish its work, ti simply transition from the WorkerState.RUNNING to WorkerState.STOPPING state. To wait for the worker exit one should call the async Worker.wait_for_shutdown method (as in the example above)

## Async Context Manager

Worker implements the async context manager protocol. Entering the context calls Worker.startup and exiting it calls Worker.shutdown, so the `async with` pattern is the most concise way to manage the full lifecycle:

```python
async with MyWorker() as worker:
    # worker is running here
    ...
# worker is fully shut down here
```

Resources that the worker needs for its entire lifetime can be opened and closed inside Worker.run using normal `async with` statements — no subclassing of lifecycle hooks is required.
