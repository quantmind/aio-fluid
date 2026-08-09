# Task Scheduler

The task scheduler TaskScheduler inherits from the TaskConsumer to add scheduling of periodic tasks.

It can be imported from `fluid.scheduler`:

```python
from fluid.scheduler import TaskScheduler
```

## fluid.scheduler.TaskScheduler

```python
TaskScheduler(
    *,
    deps=None,
    config=None,
    name="",
    stopping_grace_period=None,
    **kwargs
)
```

Bases: `TaskConsumer`

A task manager for scheduling tasks

| PARAMETER               | DESCRIPTION                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `deps`                  | Application dependencies available to every task run. See the Task Dependencies tutorial. **TYPE:** `Any` **DEFAULT:** `None`                                                                  |
| `config`                | Task manager configuration. Built from the extra keyword arguments when not provided. **TYPE:** \`TaskManagerConfig                                                                            |
| `name`                  | Worker's name, if not provided it is evaluated from the class name **TYPE:** `str` **DEFAULT:** `''`                                                                                           |
| `stopping_grace_period` | Grace period in seconds to wait for workers to stop running when this worker is shutdown. It defaults to the FLUID_STOPPING_GRACE_PERIOD environment variable or 10 seconds. **TYPE:** \`float |
| `**kwargs`              | Configuration fields, used when config is not provided. **TYPE:** `Any` **DEFAULT:** `{}`                                                                                                      |

Source code in `fluid/scheduler/scheduler.py`

```python
def __init__(
    self,
    *,
    deps: Annotated[
        Any,
        Doc("""
            Application dependencies available to every task run.

            See the [Task Dependencies](../tutorials/task_deps.md)
            tutorial.
            """),
    ] = None,
    config: Annotated[
        TaskManagerConfig | None,
        Doc("""
            Task manager configuration.

            Built from the extra keyword arguments when not provided.
            """),
    ] = None,
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
    **kwargs: Annotated[
        Any,
        Doc("Configuration fields, used when `config` is not provided."),
    ],
) -> None:
    super().__init__(
        deps=deps,
        config=config,
        name=name,
        stopping_grace_period=stopping_grace_period,
        **kwargs,
    )
    self.add_workers(ScheduleTasks(self))
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

### deps

```python
deps = deps if deps is not None else State()
```

Dependencies for the task manager.

Production applications requires global dependencies to be available to all tasks. This can be achieved by setting the `deps` attribute of the task manager to an object with the required dependencies.

Each task can cast the dependencies to the required type.

### state

```python
state = State()
```

State for the task manager. This can be used by plugins to store state in the task manager.

### config

```python
config = config or TaskManagerConfig(**kwargs)
```

Task manager configuration

### dispatcher

```python
dispatcher = TaskDispatcher()
```

A dispatcher of TaskRun events.

Application can register handlers to listen for events happening during the lifecycle of a task run.

### broker

```python
broker = TaskBroker.from_url(self.config.broker_url)
```

### manager_id

```python
manager_id = self.broker.new_uuid()
```

### registry

```python
registry
```

The task registry

### type

```python
type
```

The type of the task manager

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

Try to gracefully stop the workers and this worker

Source code in `fluid/utils/worker.py`

```python
def gracefully_stop(self) -> None:
    """Try to gracefully stop the workers and this worker"""
    super().gracefully_stop()
    for worker in self._workers:
        worker.gracefully_stop()
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
    status_workers = await asyncio.gather(
        *[worker.status() for worker in self._workers],
    )
    return {
        worker.worker_name: status
        for worker, status in zip(self._workers, status_workers, strict=False)
    }
```

### on_startup

```python
on_startup()
```

Source code in `fluid/scheduler/consumer.py`

```python
async def on_startup(self) -> None:
    await self.__aenter__()
```

### on_shutdown

```python
on_shutdown()
```

Source code in `fluid/scheduler/consumer.py`

```python
async def on_shutdown(self) -> None:
    await self.__aexit__(None, None, None)
```

### startup

```python
startup()
```

Start the task consumer workers.

A cpu bound process executes a single task and exits, it never consumes the queue. Reaching this point means the entry point ignored the `exec` command, so it cannot run cpu bound tasks.

Source code in `fluid/scheduler/consumer.py`

```python
async def startup(self) -> None:
    """Start the task consumer workers.

    A cpu bound process executes a single task and exits, it never consumes
    the queue. Reaching this point means the entry point ignored the `exec`
    command, so it cannot run cpu bound tasks.
    """
    if is_in_cpu_process():
        raise CpuBoundEntryPointError(
            "a task consumer cannot start in a cpu bound process: "
            "running cpu bound tasks requires the application entry point "
            "to be a TaskManagerCLI"
        )
    await super().startup()
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

Source code in `fluid/utils/worker.py`

```python
def workers(self) -> Iterator[Worker]:
    return iter(self._workers)
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

### add_async_context_manager

```python
add_async_context_manager(cm)
```

Add an async context manager to the task manager

These context managers are entered when the task manager starts

Source code in `fluid/scheduler/consumer.py`

```python
def add_async_context_manager(self, cm: Any) -> None:
    """Add an async context manager to the task manager

    These context managers are entered when the task manager starts
    """
    self._async_contexts.append(cm)
```

### register_task

```python
register_task(task, tags=None)
```

Register a task with the task manager

| PARAMETER | DESCRIPTION                                                                   |
| --------- | ----------------------------------------------------------------------------- |
| `task`    | Task to register **TYPE:** `Task`                                             |
| `tags`    | Extra tags to add to the task before registering it **TYPE:** \`Sequence[str] |

Source code in `fluid/scheduler/consumer.py`

```python
def register_task(
    self,
    task: Annotated[Task, Doc("Task to register")],
    tags: Annotated[
        Sequence[str] | None,
        Doc("Extra tags to add to the task before registering it"),
    ] = None,
) -> None:
    """Register a task with the task manager"""
    if tags:
        task = task._replace(tags=task.tags | frozenset(tags))
    self.broker.register_task(task)
```

### execute

```python
execute(task, *, run_id='', priority=None, **params)
```

Execute a task and wait for it to finish

This method is an async method that should be used in an asynchronous context when one need to wait for the task to finish execution.

| PARAMETER  | DESCRIPTION                                                                                                       |
| ---------- | ----------------------------------------------------------------------------------------------------------------- |
| `task`     | The task or task name, if a task name it must be registered with the task manager. **TYPE:** \`str                |
| `run_id`   | Unique ID for the task run. If not provided a new UUID is generated. **TYPE:** `str` **DEFAULT:** `''`            |
| `priority` | Override the default task priority if provided **TYPE:** \`TaskPriority                                           |
| `**params` | The optional parameters for the task run. They must match the task params model **TYPE:** `Any` **DEFAULT:** `{}` |

Source code in `fluid/scheduler/consumer.py`

```python
async def execute(
    self,
    task: Annotated[
        str | Task,
        Doc(
            "The task or task name,"
            " if a task name it must be registered with the task manager."
        ),
    ],
    *,
    run_id: Annotated[
        str,
        Doc("Unique ID for the task run. If not provided a new UUID is generated."),
    ] = "",
    priority: Annotated[
        TaskPriority | None, Doc("Override the default task priority if provided")
    ] = None,
    **params: Annotated[
        Any,
        Doc(
            "The optional parameters for the task run. "
            "They must match the task params model"
        ),
    ],
) -> TaskRun:
    """Execute a task and wait for it to finish

    This method is an async method that should be used in an asynchronous
    context when one need to wait for the task to finish execution.
    """
    task_run = self.create_task_run(
        task,
        run_id=run_id,
        priority=priority,
        **params,
    )
    try:
        await task_run._execute()
    except TaskAbortedError as exc:
        await self.broker.set_task_aborted(task_run.id, str(exc))
    return task_run
```

### execute_sync

```python
execute_sync(task, *, run_id='', priority=None, **params)
```

Execute a task synchronously

This method is a blocking method that should be used in a synchronous context.

| PARAMETER  | DESCRIPTION                                                                                                       |
| ---------- | ----------------------------------------------------------------------------------------------------------------- |
| `task`     | The task or task name, if a task name it must be registered with the task manager. **TYPE:** \`str                |
| `run_id`   | Unique ID for the task run. If not provided a new UUID is generated. **TYPE:** `str` **DEFAULT:** `''`            |
| `priority` | Override the default task priority if provided **TYPE:** \`TaskPriority                                           |
| `**params` | The optional parameters for the task run. They must match the task params model **TYPE:** `Any` **DEFAULT:** `{}` |

Source code in `fluid/scheduler/consumer.py`

```python
def execute_sync(
    self,
    task: Annotated[
        str | Task,
        Doc(
            "The task or task name,"
            " if a task name it must be registered with the task manager."
        ),
    ],
    *,
    run_id: Annotated[
        str,
        Doc("Unique ID for the task run. If not provided a new UUID is generated."),
    ] = "",
    priority: Annotated[
        TaskPriority | None, Doc("Override the default task priority if provided")
    ] = None,
    **params: Annotated[
        Any,
        Doc(
            "The optional parameters for the task run. "
            "They must match the task params model"
        ),
    ],
) -> TaskRun:
    """Execute a task synchronously

    This method is a blocking method that should be used in a synchronous
    context.
    """
    return asyncio.run(
        self._execute_and_exit(
            task,
            run_id=run_id,
            priority=priority,
            **params,
        )
    )
```

### queue

```python
queue(
    task,
    *,
    run_id="",
    priority=None,
    from_task_run=None,
    **params
)
```

Queue a task for execution

This methods fires two events:

- `init`: when the task run is created
- `queued`: after the task is queued

It returns the TaskRun object

| PARAMETER       | DESCRIPTION                                                                                                       |
| --------------- | ----------------------------------------------------------------------------------------------------------------- |
| `task`          | The task or task name, if a task name it must be registered with the task manager. **TYPE:** \`str                |
| `run_id`        | Unique ID for the task run. If not provided a new UUID is generated. **TYPE:** `str` **DEFAULT:** `''`            |
| `priority`      | Override the default task priority if provided **TYPE:** \`TaskPriority                                           |
| `from_task_run` | The task run queueing this one, if any. Prefer TaskRun.queue, which passes it for you. **TYPE:** \`TaskRun        |
| `**params`      | The optional parameters for the task run. They must match the task params model **TYPE:** `Any` **DEFAULT:** `{}` |

Source code in `fluid/scheduler/consumer.py`

```python
async def queue(
    self,
    task: Annotated[
        str | Task,
        Doc(
            "The task or task name,"
            " if a task name it must be registered with the task manager."
        ),
    ],
    *,
    run_id: Annotated[
        str,
        Doc("Unique ID for the task run. If not provided a new UUID is generated."),
    ] = "",
    priority: Annotated[
        TaskPriority | None, Doc("Override the default task priority if provided")
    ] = None,
    from_task_run: Annotated[
        TaskRun | None,
        Doc(
            "The task run queueing this one, if any. "
            "Prefer [TaskRun.queue][fluid.scheduler.TaskRun.queue], "
            "which passes it for you."
        ),
    ] = None,
    **params: Annotated[
        Any,
        Doc(
            "The optional parameters for the task run. "
            "They must match the task params model"
        ),
    ],
) -> TaskRun:
    """Queue a task for execution

    This methods fires two events:

    - `init`: when the task run is created
    - `queued`: after the task is queued

    It returns the [TaskRun][fluid.scheduler.TaskRun] object
    """
    task_run = self.create_task_run(
        task,
        run_id=run_id,
        priority=priority,
        from_task_run=from_task_run,
        **params,
    )
    return await self._queue_task_run(task_run)
```

### create_task_run

```python
create_task_run(
    task,
    *,
    run_id="",
    priority=None,
    from_task_run=None,
    **params
)
```

Create a TaskRun in `init` state

| PARAMETER       | DESCRIPTION                                                                                                       |
| --------------- | ----------------------------------------------------------------------------------------------------------------- |
| `task`          | The task or task name, if a task name it must be registered with the task manager. **TYPE:** \`str                |
| `run_id`        | Unique ID for the task run. If not provided a new UUID is generated. **TYPE:** `str` **DEFAULT:** `''`            |
| `priority`      | Override the default task priority if provided **TYPE:** \`TaskPriority                                           |
| `from_task_run` | The task run creating this one, if any. It records the chain. **TYPE:** \`TaskRun                                 |
| `**params`      | The optional parameters for the task run. They must match the task params model **TYPE:** `Any` **DEFAULT:** `{}` |

Source code in `fluid/scheduler/consumer.py`

```python
def create_task_run(
    self,
    task: Annotated[
        str | Task,
        Doc(
            "The task or task name,"
            " if a task name it must be registered with the task manager."
        ),
    ],
    *,
    run_id: Annotated[
        str,
        Doc("Unique ID for the task run. If not provided a new UUID is generated."),
    ] = "",
    priority: Annotated[
        TaskPriority | None, Doc("Override the default task priority if provided")
    ] = None,
    from_task_run: Annotated[
        TaskRun | None,
        Doc("The task run creating this one, if any. It records the chain."),
    ] = None,
    **params: Annotated[
        Any,
        Doc(
            "The optional parameters for the task run. "
            "They must match the task params model"
        ),
    ],
) -> TaskRun:
    """Create a [TaskRun][fluid.scheduler.TaskRun] in `init` state"""
    task = self.broker.task_from_registry(task)
    run_id = run_id or self.broker.new_uuid()
    return TaskRun(
        id=run_id,
        task=task,
        priority=priority or task.priority,
        params=task.params_model(**params),
        task_manager=self,
        from_run_id=from_task_run.id if from_task_run else "",
        # the first run in a chain has no root of its own, it is the root
        root_run_id=(
            (from_task_run.root_run_id or from_task_run.id) if from_task_run else ""
        ),
    )
```

### register_from_module

```python
register_from_module(module, tags=None)
```

Register tasks from a python module

| PARAMETER | DESCRIPTION                                                                                                                     |
| --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `module`  | Python module with tasks implementations - can contain any object, only instances of Task are registered **TYPE:** `ModuleType` |
| `tags`    | Extra tags to add to every registered task **TYPE:** \`Sequence[str]                                                            |

Source code in `fluid/scheduler/consumer.py`

```python
def register_from_module(
    self,
    module: Annotated[
        ModuleType,
        Doc(
            "Python module with tasks implementations "
            "- can contain any object, only instances of Task are registered"
        ),
    ],
    tags: Annotated[
        Sequence[str] | None,
        Doc("Extra tags to add to every registered task"),
    ] = None,
) -> None:
    """Register tasks from a python module"""
    for name in dir(module):
        if name.startswith("_"):
            continue
        if isinstance(obj := getattr(module, name), Task):
            self.register_task(obj, tags=tags)
```

### register_from_dict

```python
register_from_dict(data, tags=None)
```

Register tasks from a python dictionary

| PARAMETER | DESCRIPTION                                                                                                                             |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `data`    | Python dictionary with tasks implementations - can contain any object, only instances of Task are registered **TYPE:** `dict[str, Any]` |
| `tags`    | Extra tags to add to every registered task **TYPE:** \`Sequence[str]                                                                    |

Source code in `fluid/scheduler/consumer.py`

```python
def register_from_dict(
    self,
    data: Annotated[
        dict[str, Any],
        Doc(
            "Python dictionary with tasks implementations "
            "- can contain any object, only instances of Task are registered"
        ),
    ],
    tags: Annotated[
        Sequence[str] | None,
        Doc("Extra tags to add to every registered task"),
    ] = None,
) -> None:
    """Register tasks from a python dictionary"""
    for name, obj in data.items():
        if name.startswith("_"):
            continue
        if isinstance(obj, Task):
            self.register_task(obj, tags=tags)
```

### register_async_handler

```python
register_async_handler(event, handler)
```

Source code in `fluid/scheduler/consumer.py`

```python
def register_async_handler(self, event: Event | str, handler: AsyncHandler) -> None:
    event = Event.from_string_or_event(event)
    self.dispatcher.register_handler(
        f"{event.type}.async_dispatch",
        self._async_dispatcher_worker.send,
    )
    self._async_dispatcher_worker.dispatcher.register_handler(event, handler)
```

### unregister_async_handler

```python
unregister_async_handler(event)
```

Source code in `fluid/scheduler/consumer.py`

```python
def unregister_async_handler(self, event: Event | str) -> AsyncHandler | None:
    return self._async_dispatcher_worker.dispatcher.unregister_handler(event)
```

### with_plugin

```python
with_plugin(plugin)
```

Register a plugin with the task manager

| PARAMETER | DESCRIPTION                                          |
| --------- | ---------------------------------------------------- |
| `plugin`  | The plugin to register **TYPE:** `TaskManagerPlugin` |

Source code in `fluid/scheduler/consumer.py`

```python
def with_plugin(
    self,
    plugin: Annotated[TaskManagerPlugin, Doc("The plugin to register")],
) -> Self:
    """Register a plugin with the task manager"""
    self._plugins.append(plugin)
    plugin.register(self)
    return self
```

### sync_queue

```python
sync_queue(task, delay=0)
```

Queue a task synchronously

Source code in `fluid/scheduler/consumer.py`

```python
def sync_queue(self, task: str | Task | TaskRun, delay: float = 0) -> None:
    """Queue a task synchronously"""
    self._in_process_queue.queue(task, delay=delay)
```

### queue_and_wait

```python
queue_and_wait(task, *, timeout=None, **params)
```

Queue a task and wait for it to finish

| PARAMETER  | DESCRIPTION                                                                                                       |
| ---------- | ----------------------------------------------------------------------------------------------------------------- |
| `task`     | The task or task name, if a task name it must be registered with the task manager. **TYPE:** \`str                |
| `timeout`  | Timeout for waiting the task to finish **TYPE:** \`int                                                            |
| `**params` | The optional parameters for the task run. They must match the task params model **TYPE:** `Any` **DEFAULT:** `{}` |

Source code in `fluid/scheduler/consumer.py`

```python
async def queue_and_wait(
    self,
    task: Annotated[
        str | Task,
        Doc(
            "The task or task name,"
            " if a task name it must be registered with the task manager."
        ),
    ],
    *,
    timeout: Annotated[
        int | None, Doc("Timeout for waiting the task to finish")
    ] = None,
    **params: Annotated[
        Any,
        Doc(
            "The optional parameters for the task run. "
            "They must match the task params model"
        ),
    ],
) -> TaskRun:
    """Queue a task and wait for it to finish"""
    with TaskRunWaiter(self) as waiter:
        task_run = await self.queue(task, **params)
        return await waiter.wait(task_run, timeout=timeout)
```
