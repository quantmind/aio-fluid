# Workers

Workers are the main building block for asynchronous programming with `aio-fluid`. They are responsible for running asynchronous tasks and managing their lifecycle. There are several worker classes which can be imported from `fluid.utils.worker`, and they aall derive from the abstract `fluid.utils.worker.Worker` class.

```python
from fluid.utils.worker import Worker
```

## fluid.utils.worker.WorkerState

Bases: `StrEnum`

The lifecycle state of a Worker.

### INIT

```python
INIT = enum.auto()
```

Worker has been created but not yet started.

### RUNNING

```python
RUNNING = enum.auto()
```

Worker is actively executing its run loop.

### STOPPING

```python
STOPPING = enum.auto()
```

Graceful stop requested; run should exit at its next safe point.

### STOPPED

```python
STOPPED = enum.auto()
```

Worker exited cleanly after a graceful stop.

### FORCE_STOPPED

```python
FORCE_STOPPED = enum.auto()
```

Worker was cancelled because it did not exit within the grace period.

## fluid.utils.worker.Worker

```python
Worker(*, name='', stopping_grace_period=None)
```

Bases: `ABC`

Abstract base class for all workers.

A worker encapsulates a long-running async task with a managed lifecycle. Subclasses implement run, which is called once the worker is started and should loop until is_running returns `False`.

Use startup to start the worker as an asyncio task, and shutdown (or gracefully_stop + wait_for_shutdown) to stop it.

Override on_startup and on_shutdown to initialise and clean up async resources that the worker owns.

| PARAMETER               | DESCRIPTION                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                  | Worker's name, if not provided it is evaluated from the class name **TYPE:** `str` **DEFAULT:** `''`                                                                                           |
| `stopping_grace_period` | Grace period in seconds to wait for workers to stop running when this worker is shutdown. It defaults to the FLUID_STOPPING_GRACE_PERIOD environment variable or 10 seconds. **TYPE:** \`float |

Source code in `fluid/utils/worker.py`

```python
def __init__(
    self,
    *,
    name: Annotated[
        str,
        Doc("Worker's name, if not provided it is evaluated from the class name"),
    ] = "",
    stopping_grace_period: Annotated[
        float | None,
        Doc(
            "Grace period in seconds to wait for workers to stop running "
            "when this worker is shutdown. "
            "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
            "environment variable or 10 seconds."
        ),
    ] = None,
) -> None:
    if stopping_grace_period is None:
        stopping_grace_period = settings.STOPPING_GRACE_PERIOD
    self._worker_name: str = name or snake_case(type(self).__name__)
    self._worker_state: WorkerState = WorkerState.INIT
    self._stopping_grace_period = stopping_grace_period
    self._worker_task_runner: WorkerTaskRunner | None = None
```

### worker_state

```python
worker_state
```

The running state of the worker

### worker_name

```python
worker_name
```

The name of the worker

### num_workers

```python
num_workers
```

The number of workers in this worker

### has_started

```python
has_started()
```

Source code in `fluid/utils/worker.py`

```python
def has_started(self) -> bool:
    return self._worker_state != WorkerState.INIT
```

### is_running

```python
is_running()
```

Source code in `fluid/utils/worker.py`

```python
def is_running(self) -> bool:
    return self._worker_state == WorkerState.RUNNING
```

### is_stopping

```python
is_stopping()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopping(self) -> bool:
    return self._worker_state == WorkerState.STOPPING
```

### is_stopped

```python
is_stopped()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopped(self) -> bool:
    return self._worker_state in (WorkerState.STOPPED, WorkerState.FORCE_STOPPED)
```

### gracefully_stop

```python
gracefully_stop()
```

Try to gracefully stop the worker

Source code in `fluid/utils/worker.py`

```python
def gracefully_stop(self) -> None:
    """Try to gracefully stop the worker"""
    if self.is_running():
        self._worker_state = WorkerState.STOPPING
```

### after_shutdown

```python
after_shutdown(reason, code)
```

Called after shutdown of worker

By default it does nothing, but can be overriden to do something such as exit the process.

Source code in `fluid/utils/worker.py`

```python
def after_shutdown(self, reason: str, code: int) -> None:  # noqa: B027
    """Called after shutdown of worker

    By default it does nothing, but can be overriden to do something such as
    exit the process.
    """
```

### status

```python
status()
```

Source code in `fluid/utils/worker.py`

```python
async def status(self) -> dict:
    return {"stopping": self.is_stopping(), "running": self.is_running()}
```

### on_startup

```python
on_startup()
```

Called when the worker starts running

Use this function to initialize other async resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_startup(self) -> None:  # noqa: B027
    """Called when the worker starts running

    Use this function to initialize other async resources connected with the worker
    """
```

### on_shutdown

```python
on_shutdown()
```

called after the worker stopped running

Use this function to cleanup resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_shutdown(self) -> None:  # noqa: B027
    """called after the worker stopped running

    Use this function to cleanup resources connected with the worker
    """
```

### startup

```python
startup()
```

start the worker

This method creates a task to run the worker.

Source code in `fluid/utils/worker.py`

```python
async def startup(self) -> None:
    """start the worker

    This method creates a task to run the worker.
    """
    if self.has_started():
        raise WorkerStartError(
            "worker %s already started: %s", self.worker_name, self._worker_state
        )
    else:
        self._worker_task_runner = await WorkerTaskRunner.start(self)
```

### shutdown

```python
shutdown()
```

Shutdown a running worker and wait for it to stop

This method will try to gracefully stop the worker and wait for it to stop. If the worker does not stop in the grace period, it will force shutdown by cancelling the task.

Source code in `fluid/utils/worker.py`

```python
async def shutdown(self) -> None:
    """Shutdown a running worker and wait for it to stop

    This method will try to gracefully stop the worker and wait for it to stop.
    If the worker does not stop in the grace period, it will force shutdown
    by cancelling the task.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.shutdown()
```

### wait_for_shutdown

```python
wait_for_shutdown()
```

Wait for the worker to stop

This method will wait for the worker to stop running, but doesn't try to gracefully stop it nor force shutdown.

Source code in `fluid/utils/worker.py`

```python
async def wait_for_shutdown(self) -> None:
    """Wait for the worker to stop

    This method will wait for the worker to stop running, but doesn't
    try to gracefully stop it nor force shutdown.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.wait_for_shutdown()
```

### workers

```python
workers()
```

An iterator of workers in this worker

Source code in `fluid/utils/worker.py`

```python
def workers(self) -> Iterator[Worker]:
    """An iterator of workers in this worker"""
    yield self
```

### run

```python
run()
```

run the worker

This is the only abstract method and that needs implementing. It is the coroutine that mantains the worker running.

Source code in `fluid/utils/worker.py`

```python
@abstractmethod
async def run(self) -> None:
    """run the worker

    This is the only abstract method and that needs implementing.
    It is the coroutine that mantains the worker running.
    """
```

## fluid.utils.worker.WorkerFunction

```python
WorkerFunction(
    run_function,
    *,
    heartbeat=0,
    name="",
    stopping_grace_period=None
)
```

Bases: `Worker`

A Worker that calls a coroutine function in a loop.

On each iteration the supplied `run_function` is awaited, then the worker sleeps for `heartbeat` seconds before repeating. The loop exits when is_running returns `False`.

| PARAMETER               | DESCRIPTION                                                                                                             |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `run_function`          | The coroutine function tuo run and await at each iteration of the worker loop **TYPE:** `Callable[[], Awaitable[None]]` |
| `heartbeat`             | The time to wait between each coroutine function run **TYPE:** \`float                                                  |
| `name`                  | Worker's name, if not provided it is evaluated from the class name **TYPE:** `str` **DEFAULT:** `''`                    |
| `stopping_grace_period` | Grace period in seconds before force-cancelling this worker **TYPE:** \`float                                           |

Source code in `fluid/utils/worker.py`

```python
def __init__(
    self,
    run_function: Annotated[
        Callable[[], Awaitable[None]],
        Doc(
            "The coroutine function tuo run and await at each iteration "
            "of the worker loop"
        ),
    ],
    *,
    heartbeat: Annotated[
        float | int, Doc("The time to wait between each coroutine function run")
    ] = 0,
    name: Annotated[
        str,
        Doc("Worker's name, if not provided it is evaluated from the class name"),
    ] = "",
    stopping_grace_period: Annotated[
        float | None,
        Doc("Grace period in seconds before force-cancelling this worker"),
    ] = None,
) -> None:
    super().__init__(name=name, stopping_grace_period=stopping_grace_period)
    self._run_function = run_function
    self._heartbeat = heartbeat
```

### worker_state

```python
worker_state
```

The running state of the worker

### worker_name

```python
worker_name
```

The name of the worker

### num_workers

```python
num_workers
```

The number of workers in this worker

### run

```python
run()
```

Source code in `fluid/utils/worker.py`

```python
async def run(self) -> None:
    while self.is_running():
        await self._run_function()
        await asyncio.sleep(self._heartbeat)
```

### has_started

```python
has_started()
```

Source code in `fluid/utils/worker.py`

```python
def has_started(self) -> bool:
    return self._worker_state != WorkerState.INIT
```

### is_running

```python
is_running()
```

Source code in `fluid/utils/worker.py`

```python
def is_running(self) -> bool:
    return self._worker_state == WorkerState.RUNNING
```

### is_stopping

```python
is_stopping()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopping(self) -> bool:
    return self._worker_state == WorkerState.STOPPING
```

### is_stopped

```python
is_stopped()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopped(self) -> bool:
    return self._worker_state in (WorkerState.STOPPED, WorkerState.FORCE_STOPPED)
```

### gracefully_stop

```python
gracefully_stop()
```

Try to gracefully stop the worker

Source code in `fluid/utils/worker.py`

```python
def gracefully_stop(self) -> None:
    """Try to gracefully stop the worker"""
    if self.is_running():
        self._worker_state = WorkerState.STOPPING
```

### after_shutdown

```python
after_shutdown(reason, code)
```

Called after shutdown of worker

By default it does nothing, but can be overriden to do something such as exit the process.

Source code in `fluid/utils/worker.py`

```python
def after_shutdown(self, reason: str, code: int) -> None:  # noqa: B027
    """Called after shutdown of worker

    By default it does nothing, but can be overriden to do something such as
    exit the process.
    """
```

### status

```python
status()
```

Source code in `fluid/utils/worker.py`

```python
async def status(self) -> dict:
    return {"stopping": self.is_stopping(), "running": self.is_running()}
```

### on_startup

```python
on_startup()
```

Called when the worker starts running

Use this function to initialize other async resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_startup(self) -> None:  # noqa: B027
    """Called when the worker starts running

    Use this function to initialize other async resources connected with the worker
    """
```

### on_shutdown

```python
on_shutdown()
```

called after the worker stopped running

Use this function to cleanup resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_shutdown(self) -> None:  # noqa: B027
    """called after the worker stopped running

    Use this function to cleanup resources connected with the worker
    """
```

### startup

```python
startup()
```

start the worker

This method creates a task to run the worker.

Source code in `fluid/utils/worker.py`

```python
async def startup(self) -> None:
    """start the worker

    This method creates a task to run the worker.
    """
    if self.has_started():
        raise WorkerStartError(
            "worker %s already started: %s", self.worker_name, self._worker_state
        )
    else:
        self._worker_task_runner = await WorkerTaskRunner.start(self)
```

### shutdown

```python
shutdown()
```

Shutdown a running worker and wait for it to stop

This method will try to gracefully stop the worker and wait for it to stop. If the worker does not stop in the grace period, it will force shutdown by cancelling the task.

Source code in `fluid/utils/worker.py`

```python
async def shutdown(self) -> None:
    """Shutdown a running worker and wait for it to stop

    This method will try to gracefully stop the worker and wait for it to stop.
    If the worker does not stop in the grace period, it will force shutdown
    by cancelling the task.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.shutdown()
```

### wait_for_shutdown

```python
wait_for_shutdown()
```

Wait for the worker to stop

This method will wait for the worker to stop running, but doesn't try to gracefully stop it nor force shutdown.

Source code in `fluid/utils/worker.py`

```python
async def wait_for_shutdown(self) -> None:
    """Wait for the worker to stop

    This method will wait for the worker to stop running, but doesn't
    try to gracefully stop it nor force shutdown.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.wait_for_shutdown()
```

### workers

```python
workers()
```

An iterator of workers in this worker

Source code in `fluid/utils/worker.py`

```python
def workers(self) -> Iterator[Worker]:
    """An iterator of workers in this worker"""
    yield self
```

## fluid.utils.worker.QueueConsumer

```python
QueueConsumer(*, name='', stopping_grace_period=None)
```

Bases: `Worker`, `MessageProducer[MessageType]`

Abstract Worker backed by an asyncio queue.

Provides send for thread-safe message delivery and get_message for retrieving the next message with a timeout. Subclasses implement run to consume messages from the queue.

| PARAMETER               | DESCRIPTION                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                  | Worker's name, if not provided it is evaluated from the class name **TYPE:** `str` **DEFAULT:** `''`                                                                                           |
| `stopping_grace_period` | Grace period in seconds to wait for workers to stop running when this worker is shutdown. It defaults to the FLUID_STOPPING_GRACE_PERIOD environment variable or 10 seconds. **TYPE:** \`float |

Source code in `fluid/utils/worker.py`

```python
def __init__(
    self,
    *,
    name: Annotated[
        str,
        Doc("Worker's name, if not provided it is evaluated from the class name"),
    ] = "",
    stopping_grace_period: Annotated[
        float | None,
        Doc(
            "Grace period in seconds to wait for workers to stop running "
            "when this worker is shutdown. "
            "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
            "environment variable or 10 seconds."
        ),
    ] = None,
) -> None:
    super().__init__(name=name, stopping_grace_period=stopping_grace_period)
    self._queue: asyncio.Queue[MessageType | None] = asyncio.Queue()
```

### worker_state

```python
worker_state
```

The running state of the worker

### worker_name

```python
worker_name
```

The name of the worker

### num_workers

```python
num_workers
```

The number of workers in this worker

### get_message

```python
get_message(timeout=0.5)
```

Get the next message from the queue

Source code in `fluid/utils/worker.py`

```python
async def get_message(self, timeout: float = 0.5) -> MessageType | None:
    """Get the next message from the queue"""
    try:
        async with asyncio.timeout(timeout):
            return await self._queue.get()
    except asyncio.TimeoutError:
        return None
    except (asyncio.CancelledError, RuntimeError):
        if not self.is_stopping():
            raise
    return None
```

### queue_size

```python
queue_size()
```

Get the size of the queue

Source code in `fluid/utils/worker.py`

```python
def queue_size(self) -> int:
    """Get the size of the queue"""
    return self._queue.qsize()
```

### status

```python
status()
```

Source code in `fluid/utils/worker.py`

```python
async def status(self) -> dict:
    status = await super().status()
    status.update(queue_size=self.queue_size())
    return status
```

### send

```python
send(message)
```

Send a message into the worker

Source code in `fluid/utils/worker.py`

```python
def send(self, message: MessageType | None) -> None:
    """Send a message into the worker"""
    self._queue.put_nowait(message)
```

### has_started

```python
has_started()
```

Source code in `fluid/utils/worker.py`

```python
def has_started(self) -> bool:
    return self._worker_state != WorkerState.INIT
```

### is_running

```python
is_running()
```

Source code in `fluid/utils/worker.py`

```python
def is_running(self) -> bool:
    return self._worker_state == WorkerState.RUNNING
```

### is_stopping

```python
is_stopping()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopping(self) -> bool:
    return self._worker_state == WorkerState.STOPPING
```

### is_stopped

```python
is_stopped()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopped(self) -> bool:
    return self._worker_state in (WorkerState.STOPPED, WorkerState.FORCE_STOPPED)
```

### gracefully_stop

```python
gracefully_stop()
```

Try to gracefully stop the worker

Source code in `fluid/utils/worker.py`

```python
def gracefully_stop(self) -> None:
    """Try to gracefully stop the worker"""
    if self.is_running():
        self._worker_state = WorkerState.STOPPING
```

### after_shutdown

```python
after_shutdown(reason, code)
```

Called after shutdown of worker

By default it does nothing, but can be overriden to do something such as exit the process.

Source code in `fluid/utils/worker.py`

```python
def after_shutdown(self, reason: str, code: int) -> None:  # noqa: B027
    """Called after shutdown of worker

    By default it does nothing, but can be overriden to do something such as
    exit the process.
    """
```

### on_startup

```python
on_startup()
```

Called when the worker starts running

Use this function to initialize other async resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_startup(self) -> None:  # noqa: B027
    """Called when the worker starts running

    Use this function to initialize other async resources connected with the worker
    """
```

### on_shutdown

```python
on_shutdown()
```

called after the worker stopped running

Use this function to cleanup resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_shutdown(self) -> None:  # noqa: B027
    """called after the worker stopped running

    Use this function to cleanup resources connected with the worker
    """
```

### startup

```python
startup()
```

start the worker

This method creates a task to run the worker.

Source code in `fluid/utils/worker.py`

```python
async def startup(self) -> None:
    """start the worker

    This method creates a task to run the worker.
    """
    if self.has_started():
        raise WorkerStartError(
            "worker %s already started: %s", self.worker_name, self._worker_state
        )
    else:
        self._worker_task_runner = await WorkerTaskRunner.start(self)
```

### shutdown

```python
shutdown()
```

Shutdown a running worker and wait for it to stop

This method will try to gracefully stop the worker and wait for it to stop. If the worker does not stop in the grace period, it will force shutdown by cancelling the task.

Source code in `fluid/utils/worker.py`

```python
async def shutdown(self) -> None:
    """Shutdown a running worker and wait for it to stop

    This method will try to gracefully stop the worker and wait for it to stop.
    If the worker does not stop in the grace period, it will force shutdown
    by cancelling the task.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.shutdown()
```

### wait_for_shutdown

```python
wait_for_shutdown()
```

Wait for the worker to stop

This method will wait for the worker to stop running, but doesn't try to gracefully stop it nor force shutdown.

Source code in `fluid/utils/worker.py`

```python
async def wait_for_shutdown(self) -> None:
    """Wait for the worker to stop

    This method will wait for the worker to stop running, but doesn't
    try to gracefully stop it nor force shutdown.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.wait_for_shutdown()
```

### workers

```python
workers()
```

An iterator of workers in this worker

Source code in `fluid/utils/worker.py`

```python
def workers(self) -> Iterator[Worker]:
    """An iterator of workers in this worker"""
    yield self
```

### run

```python
run()
```

run the worker

This is the only abstract method and that needs implementing. It is the coroutine that mantains the worker running.

Source code in `fluid/utils/worker.py`

```python
@abstractmethod
async def run(self) -> None:
    """run the worker

    This is the only abstract method and that needs implementing.
    It is the coroutine that mantains the worker running.
    """
```

## fluid.utils.worker.QueueConsumerWorker

```python
QueueConsumerWorker(
    on_message, *, name="", stopping_grace_period=None
)
```

Bases: `QueueConsumer[MessageType]`

A QueueConsumer that dispatches each message to a single async callback.

| PARAMETER               | DESCRIPTION                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `on_message`            | The async callback to call when a message is received **TYPE:** `Callable[[MessageType], Awaitable[None]]`                                                                                     |
| `name`                  | Worker's name, if not provided it is evaluated from the class name **TYPE:** `str` **DEFAULT:** `''`                                                                                           |
| `stopping_grace_period` | Grace period in seconds to wait for workers to stop running when this worker is shutdown. It defaults to the FLUID_STOPPING_GRACE_PERIOD environment variable or 10 seconds. **TYPE:** \`float |

Source code in `fluid/utils/worker.py`

```python
def __init__(
    self,
    on_message: Annotated[
        Callable[[MessageType], Awaitable[None]],
        Doc("The async callback to call when a message is received"),
    ],
    *,
    name: Annotated[
        str,
        Doc("Worker's name, if not provided it is evaluated from the class name"),
    ] = "",
    stopping_grace_period: Annotated[
        float | None,
        Doc(
            "Grace period in seconds to wait for workers to stop running "
            "when this worker is shutdown. "
            "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
            "environment variable or 10 seconds."
        ),
    ] = None,
) -> None:
    super().__init__(name=name, stopping_grace_period=stopping_grace_period)
    self.on_message = on_message
```

### on_message

```python
on_message = on_message
```

### worker_state

```python
worker_state
```

The running state of the worker

### worker_name

```python
worker_name
```

The name of the worker

### num_workers

```python
num_workers
```

The number of workers in this worker

### run

```python
run()
```

Source code in `fluid/utils/worker.py`

```python
async def run(self) -> None:
    while not self.is_stopping():
        message = await self.get_message()
        if message is not None:
            await self.on_message(message)
```

### send

```python
send(message)
```

Send a message into the worker

Source code in `fluid/utils/worker.py`

```python
def send(self, message: MessageType | None) -> None:
    """Send a message into the worker"""
    self._queue.put_nowait(message)
```

### has_started

```python
has_started()
```

Source code in `fluid/utils/worker.py`

```python
def has_started(self) -> bool:
    return self._worker_state != WorkerState.INIT
```

### is_running

```python
is_running()
```

Source code in `fluid/utils/worker.py`

```python
def is_running(self) -> bool:
    return self._worker_state == WorkerState.RUNNING
```

### is_stopping

```python
is_stopping()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopping(self) -> bool:
    return self._worker_state == WorkerState.STOPPING
```

### is_stopped

```python
is_stopped()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopped(self) -> bool:
    return self._worker_state in (WorkerState.STOPPED, WorkerState.FORCE_STOPPED)
```

### gracefully_stop

```python
gracefully_stop()
```

Try to gracefully stop the worker

Source code in `fluid/utils/worker.py`

```python
def gracefully_stop(self) -> None:
    """Try to gracefully stop the worker"""
    if self.is_running():
        self._worker_state = WorkerState.STOPPING
```

### after_shutdown

```python
after_shutdown(reason, code)
```

Called after shutdown of worker

By default it does nothing, but can be overriden to do something such as exit the process.

Source code in `fluid/utils/worker.py`

```python
def after_shutdown(self, reason: str, code: int) -> None:  # noqa: B027
    """Called after shutdown of worker

    By default it does nothing, but can be overriden to do something such as
    exit the process.
    """
```

### status

```python
status()
```

Source code in `fluid/utils/worker.py`

```python
async def status(self) -> dict:
    status = await super().status()
    status.update(queue_size=self.queue_size())
    return status
```

### on_startup

```python
on_startup()
```

Called when the worker starts running

Use this function to initialize other async resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_startup(self) -> None:  # noqa: B027
    """Called when the worker starts running

    Use this function to initialize other async resources connected with the worker
    """
```

### on_shutdown

```python
on_shutdown()
```

called after the worker stopped running

Use this function to cleanup resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_shutdown(self) -> None:  # noqa: B027
    """called after the worker stopped running

    Use this function to cleanup resources connected with the worker
    """
```

### startup

```python
startup()
```

start the worker

This method creates a task to run the worker.

Source code in `fluid/utils/worker.py`

```python
async def startup(self) -> None:
    """start the worker

    This method creates a task to run the worker.
    """
    if self.has_started():
        raise WorkerStartError(
            "worker %s already started: %s", self.worker_name, self._worker_state
        )
    else:
        self._worker_task_runner = await WorkerTaskRunner.start(self)
```

### shutdown

```python
shutdown()
```

Shutdown a running worker and wait for it to stop

This method will try to gracefully stop the worker and wait for it to stop. If the worker does not stop in the grace period, it will force shutdown by cancelling the task.

Source code in `fluid/utils/worker.py`

```python
async def shutdown(self) -> None:
    """Shutdown a running worker and wait for it to stop

    This method will try to gracefully stop the worker and wait for it to stop.
    If the worker does not stop in the grace period, it will force shutdown
    by cancelling the task.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.shutdown()
```

### wait_for_shutdown

```python
wait_for_shutdown()
```

Wait for the worker to stop

This method will wait for the worker to stop running, but doesn't try to gracefully stop it nor force shutdown.

Source code in `fluid/utils/worker.py`

```python
async def wait_for_shutdown(self) -> None:
    """Wait for the worker to stop

    This method will wait for the worker to stop running, but doesn't
    try to gracefully stop it nor force shutdown.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.wait_for_shutdown()
```

### workers

```python
workers()
```

An iterator of workers in this worker

Source code in `fluid/utils/worker.py`

```python
def workers(self) -> Iterator[Worker]:
    """An iterator of workers in this worker"""
    yield self
```

### get_message

```python
get_message(timeout=0.5)
```

Get the next message from the queue

Source code in `fluid/utils/worker.py`

```python
async def get_message(self, timeout: float = 0.5) -> MessageType | None:
    """Get the next message from the queue"""
    try:
        async with asyncio.timeout(timeout):
            return await self._queue.get()
    except asyncio.TimeoutError:
        return None
    except (asyncio.CancelledError, RuntimeError):
        if not self.is_stopping():
            raise
    return None
```

### queue_size

```python
queue_size()
```

Get the size of the queue

Source code in `fluid/utils/worker.py`

```python
def queue_size(self) -> int:
    """Get the size of the queue"""
    return self._queue.qsize()
```

## fluid.utils.worker.AsyncConsumer

```python
AsyncConsumer(
    dispatcher, *, name="", stopping_grace_period=None
)
```

Bases: `QueueConsumer[MessageType]`

A QueueConsumer that fans out each message to all registered async handlers via an AsyncDispatcher.

The run loop processes messages until is_stopping returns `True`. Any messages remaining in the queue when the worker stops are discarded; callers that need guaranteed delivery should drain the queue before requesting a stop.

| PARAMETER               | DESCRIPTION                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dispatcher`            | Async message dispatcher to dispatch messages **TYPE:** `AsyncDispatcher[MessageType]`                                                                                                         |
| `name`                  | Worker's name, if not provided it is evaluated from the class name **TYPE:** `str` **DEFAULT:** `''`                                                                                           |
| `stopping_grace_period` | Grace period in seconds to wait for workers to stop running when this worker is shutdown. It defaults to the FLUID_STOPPING_GRACE_PERIOD environment variable or 10 seconds. **TYPE:** \`float |

Source code in `fluid/utils/worker.py`

```python
def __init__(
    self,
    dispatcher: Annotated[
        AsyncDispatcher[MessageType],
        Doc("Async message dispatcher to dispatch messages"),
    ],
    *,
    name: Annotated[
        str,
        Doc("Worker's name, if not provided it is evaluated from the class name"),
    ] = "",
    stopping_grace_period: Annotated[
        float | None,
        Doc(
            "Grace period in seconds to wait for workers to stop running "
            "when this worker is shutdown. "
            "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
            "environment variable or 10 seconds."
        ),
    ] = None,
) -> None:
    super().__init__(name=name, stopping_grace_period=stopping_grace_period)
    self.dispatcher: AsyncDispatcher[MessageType] = dispatcher
```

### dispatcher

```python
dispatcher = dispatcher
```

### worker_state

```python
worker_state
```

The running state of the worker

### worker_name

```python
worker_name
```

The name of the worker

### num_workers

```python
num_workers
```

The number of workers in this worker

### run

```python
run()
```

Source code in `fluid/utils/worker.py`

```python
async def run(self) -> None:
    while not self.is_stopping():
        message = await self.get_message()
        if message is not None:
            await self.dispatcher.dispatch(message)
```

### send

```python
send(message)
```

Send a message into the worker

Source code in `fluid/utils/worker.py`

```python
def send(self, message: MessageType | None) -> None:
    """Send a message into the worker"""
    self._queue.put_nowait(message)
```

### has_started

```python
has_started()
```

Source code in `fluid/utils/worker.py`

```python
def has_started(self) -> bool:
    return self._worker_state != WorkerState.INIT
```

### is_running

```python
is_running()
```

Source code in `fluid/utils/worker.py`

```python
def is_running(self) -> bool:
    return self._worker_state == WorkerState.RUNNING
```

### is_stopping

```python
is_stopping()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopping(self) -> bool:
    return self._worker_state == WorkerState.STOPPING
```

### is_stopped

```python
is_stopped()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopped(self) -> bool:
    return self._worker_state in (WorkerState.STOPPED, WorkerState.FORCE_STOPPED)
```

### gracefully_stop

```python
gracefully_stop()
```

Try to gracefully stop the worker

Source code in `fluid/utils/worker.py`

```python
def gracefully_stop(self) -> None:
    """Try to gracefully stop the worker"""
    if self.is_running():
        self._worker_state = WorkerState.STOPPING
```

### after_shutdown

```python
after_shutdown(reason, code)
```

Called after shutdown of worker

By default it does nothing, but can be overriden to do something such as exit the process.

Source code in `fluid/utils/worker.py`

```python
def after_shutdown(self, reason: str, code: int) -> None:  # noqa: B027
    """Called after shutdown of worker

    By default it does nothing, but can be overriden to do something such as
    exit the process.
    """
```

### status

```python
status()
```

Source code in `fluid/utils/worker.py`

```python
async def status(self) -> dict:
    status = await super().status()
    status.update(queue_size=self.queue_size())
    return status
```

### on_startup

```python
on_startup()
```

Called when the worker starts running

Use this function to initialize other async resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_startup(self) -> None:  # noqa: B027
    """Called when the worker starts running

    Use this function to initialize other async resources connected with the worker
    """
```

### on_shutdown

```python
on_shutdown()
```

called after the worker stopped running

Use this function to cleanup resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_shutdown(self) -> None:  # noqa: B027
    """called after the worker stopped running

    Use this function to cleanup resources connected with the worker
    """
```

### startup

```python
startup()
```

start the worker

This method creates a task to run the worker.

Source code in `fluid/utils/worker.py`

```python
async def startup(self) -> None:
    """start the worker

    This method creates a task to run the worker.
    """
    if self.has_started():
        raise WorkerStartError(
            "worker %s already started: %s", self.worker_name, self._worker_state
        )
    else:
        self._worker_task_runner = await WorkerTaskRunner.start(self)
```

### shutdown

```python
shutdown()
```

Shutdown a running worker and wait for it to stop

This method will try to gracefully stop the worker and wait for it to stop. If the worker does not stop in the grace period, it will force shutdown by cancelling the task.

Source code in `fluid/utils/worker.py`

```python
async def shutdown(self) -> None:
    """Shutdown a running worker and wait for it to stop

    This method will try to gracefully stop the worker and wait for it to stop.
    If the worker does not stop in the grace period, it will force shutdown
    by cancelling the task.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.shutdown()
```

### wait_for_shutdown

```python
wait_for_shutdown()
```

Wait for the worker to stop

This method will wait for the worker to stop running, but doesn't try to gracefully stop it nor force shutdown.

Source code in `fluid/utils/worker.py`

```python
async def wait_for_shutdown(self) -> None:
    """Wait for the worker to stop

    This method will wait for the worker to stop running, but doesn't
    try to gracefully stop it nor force shutdown.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.wait_for_shutdown()
```

### workers

```python
workers()
```

An iterator of workers in this worker

Source code in `fluid/utils/worker.py`

```python
def workers(self) -> Iterator[Worker]:
    """An iterator of workers in this worker"""
    yield self
```

### get_message

```python
get_message(timeout=0.5)
```

Get the next message from the queue

Source code in `fluid/utils/worker.py`

```python
async def get_message(self, timeout: float = 0.5) -> MessageType | None:
    """Get the next message from the queue"""
    try:
        async with asyncio.timeout(timeout):
            return await self._queue.get()
    except asyncio.TimeoutError:
        return None
    except (asyncio.CancelledError, RuntimeError):
        if not self.is_stopping():
            raise
    return None
```

### queue_size

```python
queue_size()
```

Get the size of the queue

Source code in `fluid/utils/worker.py`

```python
def queue_size(self) -> int:
    """Get the size of the queue"""
    return self._queue.qsize()
```

## fluid.utils.worker.Workers

```python
Workers(
    *workers,
    name="",
    heartbeat=0.1,
    stopping_grace_period=None
)
```

Bases: `Worker`

A Worker that owns and manages a collection of child workers.

Child workers are registered with add_workers. When the `Workers` instance starts, its run loop starts each child worker and monitors their health — if any child stops unexpectedly the whole group is gracefully stopped.

On shutdown all child workers are stopped concurrently. Workers that do not exit within `stopping_grace_period` seconds are force-cancelled.

| PARAMETER               | DESCRIPTION                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `*workers`              | Workers to manage, they can also be added later via add_workers method **TYPE:** `Worker` **DEFAULT:** `()`                                                                                    |
| `name`                  | Worker's name, if not provided it is evaluated from the class name **TYPE:** `str` **DEFAULT:** `''`                                                                                           |
| `heartbeat`             | The time to wait between each workers status check **TYPE:** \`float                                                                                                                           |
| `stopping_grace_period` | Grace period in seconds to wait for workers to stop running when this worker is shutdown. It defaults to the FLUID_STOPPING_GRACE_PERIOD environment variable or 10 seconds. **TYPE:** \`float |

Source code in `fluid/utils/worker.py`

```python
def __init__(
    self,
    *workers: Annotated[
        Worker,
        Doc(
            "Workers to manage, they can also be added later "
            "via `add_workers` method"
        ),
    ],
    name: Annotated[
        str,
        Doc("Worker's name, if not provided it is evaluated from the class name"),
    ] = "",
    heartbeat: Annotated[
        float | int,
        Doc("The time to wait between each workers status check"),
    ] = 0.1,
    stopping_grace_period: Annotated[
        float | None,
        Doc(
            "Grace period in seconds to wait for workers to stop running "
            "when this worker is shutdown. "
            "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
            "environment variable or 10 seconds."
        ),
    ] = None,
) -> None:
    super().__init__(name=name, stopping_grace_period=stopping_grace_period)
    self._heartbeat = heartbeat
    self._workers: list[Worker] = []
    self.add_workers(*workers)
```

### num_workers

```python
num_workers
```

### worker_state

```python
worker_state
```

The running state of the worker

### worker_name

```python
worker_name
```

The name of the worker

### add_workers

```python
add_workers(*workers)
```

add workers to the workers

They can be added while the worker is running.

Source code in `fluid/utils/worker.py`

```python
def add_workers(self, *workers: Worker) -> None:
    """add workers to the workers

    They can be added while the worker is running.
    """
    for worker in workers:
        if worker not in self._workers:
            self._workers.append(worker)
```

### workers

```python
workers()
```

Source code in `fluid/utils/worker.py`

```python
def workers(self) -> Iterator[Worker]:
    return iter(self._workers)
```

### gracefully_stop

```python
gracefully_stop()
```

Try to gracefully stop the workers and this worker

Source code in `fluid/utils/worker.py`

```python
def gracefully_stop(self) -> None:
    """Try to gracefully stop the workers and this worker"""
    super().gracefully_stop()
    for worker in self._workers:
        worker.gracefully_stop()
```

### status

```python
status()
```

Source code in `fluid/utils/worker.py`

```python
async def status(self) -> dict:
    status_workers = await asyncio.gather(
        *[worker.status() for worker in self._workers],
    )
    return {
        worker.worker_name: status
        for worker, status in zip(self._workers, status_workers, strict=False)
    }
```

### run

```python
run()
```

Source code in `fluid/utils/worker.py`

```python
async def run(self) -> None:
    while self.is_running():
        for worker in self._workers:
            if not worker.has_started():
                await worker.startup()
            if not worker.is_running():
                self.gracefully_stop()
                break
        await asyncio.sleep(self._heartbeat)
    await self._wait_for_workers()
```

### has_started

```python
has_started()
```

Source code in `fluid/utils/worker.py`

```python
def has_started(self) -> bool:
    return self._worker_state != WorkerState.INIT
```

### is_running

```python
is_running()
```

Source code in `fluid/utils/worker.py`

```python
def is_running(self) -> bool:
    return self._worker_state == WorkerState.RUNNING
```

### is_stopping

```python
is_stopping()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopping(self) -> bool:
    return self._worker_state == WorkerState.STOPPING
```

### is_stopped

```python
is_stopped()
```

Source code in `fluid/utils/worker.py`

```python
def is_stopped(self) -> bool:
    return self._worker_state in (WorkerState.STOPPED, WorkerState.FORCE_STOPPED)
```

### after_shutdown

```python
after_shutdown(reason, code)
```

Called after shutdown of worker

By default it does nothing, but can be overriden to do something such as exit the process.

Source code in `fluid/utils/worker.py`

```python
def after_shutdown(self, reason: str, code: int) -> None:  # noqa: B027
    """Called after shutdown of worker

    By default it does nothing, but can be overriden to do something such as
    exit the process.
    """
```

### on_startup

```python
on_startup()
```

Called when the worker starts running

Use this function to initialize other async resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_startup(self) -> None:  # noqa: B027
    """Called when the worker starts running

    Use this function to initialize other async resources connected with the worker
    """
```

### on_shutdown

```python
on_shutdown()
```

called after the worker stopped running

Use this function to cleanup resources connected with the worker

Source code in `fluid/utils/worker.py`

```python
async def on_shutdown(self) -> None:  # noqa: B027
    """called after the worker stopped running

    Use this function to cleanup resources connected with the worker
    """
```

### startup

```python
startup()
```

start the worker

This method creates a task to run the worker.

Source code in `fluid/utils/worker.py`

```python
async def startup(self) -> None:
    """start the worker

    This method creates a task to run the worker.
    """
    if self.has_started():
        raise WorkerStartError(
            "worker %s already started: %s", self.worker_name, self._worker_state
        )
    else:
        self._worker_task_runner = await WorkerTaskRunner.start(self)
```

### shutdown

```python
shutdown()
```

Shutdown a running worker and wait for it to stop

This method will try to gracefully stop the worker and wait for it to stop. If the worker does not stop in the grace period, it will force shutdown by cancelling the task.

Source code in `fluid/utils/worker.py`

```python
async def shutdown(self) -> None:
    """Shutdown a running worker and wait for it to stop

    This method will try to gracefully stop the worker and wait for it to stop.
    If the worker does not stop in the grace period, it will force shutdown
    by cancelling the task.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.shutdown()
```

### wait_for_shutdown

```python
wait_for_shutdown()
```

Wait for the worker to stop

This method will wait for the worker to stop running, but doesn't try to gracefully stop it nor force shutdown.

Source code in `fluid/utils/worker.py`

```python
async def wait_for_shutdown(self) -> None:
    """Wait for the worker to stop

    This method will wait for the worker to stop running, but doesn't
    try to gracefully stop it nor force shutdown.
    """
    if self._worker_task_runner is not None:
        await self._worker_task_runner.wait_for_shutdown()
```
