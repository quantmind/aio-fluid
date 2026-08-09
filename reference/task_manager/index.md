# Task Manager

The Task Manager is a component that manages the execution of tasks. It is the simplest way to run tasks and it is the base class for the TaskConsumer and the TaskScheduler.

It can be imported from `fluid.scheduler`:

```python
from fluid.scheduler import TaskManager
```

The Task Manager is useful if you want to execute tasks in a synchronous or asynchronous way.

## fluid.scheduler.TaskManager

```python
TaskManager(*, deps=None, config=None, **kwargs)
```

The task manager is the main class for managing tasks

| PARAMETER  | DESCRIPTION                                                                                                                   |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `deps`     | Application dependencies available to every task run. See the Task Dependencies tutorial. **TYPE:** `Any` **DEFAULT:** `None` |
| `config`   | Task manager configuration. Built from the extra keyword arguments when not provided. **TYPE:** \`TaskManagerConfig           |
| `**kwargs` | Configuration fields, used when config is not provided. **TYPE:** `Any` **DEFAULT:** `{}`                                     |

Source code in `fluid/scheduler/consumer.py`

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
    **kwargs: Annotated[
        Any,
        Doc("Configuration fields, used when `config` is not provided."),
    ],
) -> None:
    self.deps: Annotated[
        Any,
        Doc("""
            Dependencies for the task manager.

            Production applications requires global dependencies to be
            available to all tasks. This can be achieved by setting
            the `deps` attribute of the task manager to an object
            with the required dependencies.

            Each task can cast the dependencies to the required type.
            """),
    ] = (
        deps if deps is not None else State()
    )
    self.state: Annotated[
        State,
        Doc("""
            State for the task manager.
            This can be used by plugins to store state in the task manager.
            """),
    ] = State()
    self.config: Annotated[
        TaskManagerConfig, Doc("""Task manager configuration""")
    ] = config or TaskManagerConfig(**kwargs)
    self.dispatcher: Annotated[
        TaskDispatcher,
        Doc("""
            A dispatcher of [TaskRun][fluid.scheduler.TaskRun] events.

            Application can register handlers to listen for events
            happening during the lifecycle of a task run.
            """),
    ] = TaskDispatcher()
    self.broker = TaskBroker.from_url(self.config.broker_url)
    self.manager_id: str = self.broker.new_uuid()
    self._plugins: list[TaskManagerPlugin] = []
    self._async_contexts: list[Any] = []
    self._stack = AsyncExitStack()
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

Register an async handler for a given event

This method is a no op for a TaskManager that is not a worker

| PARAMETER | DESCRIPTION                                             |
| --------- | ------------------------------------------------------- |
| `event`   | The event to register the handler for **TYPE:** \`Event |

Source code in `fluid/scheduler/consumer.py`

```python
def register_async_handler(
    self,
    event: Annotated[Event | str, Doc("The event to register the handler for")],
    handler: AsyncHandler,
) -> None:
    """Register an async handler for a given event

    This method is a no op for a TaskManager that is not a worker
    """
```

### unregister_async_handler

```python
unregister_async_handler(event)
```

Unregister an async handler for a given event

This method is a no op for a TaskManager that is not a worker

| PARAMETER | DESCRIPTION                                               |
| --------- | --------------------------------------------------------- |
| `event`   | The event to unregister the handler for **TYPE:** \`Event |

Source code in `fluid/scheduler/consumer.py`

```python
def unregister_async_handler(
    self,
    event: Annotated[Event | str, Doc("The event to unregister the handler for")],
) -> AsyncHandler | None:
    """Unregister an async handler for a given event

    This method is a no op for a TaskManager that is not a worker
    """
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

## fluid.scheduler.TaskManagerConfig

Bases: `BaseModel`

Task manager configuration

Fields:

- `schedule_tasks` (`bool`)
- `consume_tasks` (`bool`)
- `max_concurrent_tasks` (`int`)
- `sleep_millis` (`int`)
- `broker_url` (`str`)

### schedule_tasks

```python
schedule_tasks
```

Schedule tasks or sleep

### consume_tasks

```python
consume_tasks = True
```

Consume tasks or sleep

### max_concurrent_tasks

```python
max_concurrent_tasks
```

The number of coroutine workers consuming tasks. Each worker consumes one task at a time, therefore, this number is the maximum number of tasks that can run concurrently.It can be configured via the `FLUID_MAX_CONCURRENT_TASKS` environment variable, and by default is set to 5.

### sleep_millis

```python
sleep_millis
```

Milliseconds to async sleep when no tasks available to consume.This value can be configured via the `FLUID_SLEEP_MILLIS` environment variable, and by default is set to 1000 milliseconds (1 second).

### broker_url

```python
broker_url = ''
```

### sleep

```python
sleep
```

Sleep time in seconds

## fluid.scheduler.consumer.TaskDispatcher

```python
TaskDispatcher()
```

Bases: `Dispatcher[TaskRun]`

The task dispatcher is responsible for dispatching task run messages

Source code in `fluid/utils/dispatcher.py`

```python
def __init__(self) -> None:
    self._msg_handlers: defaultdict[str, dict[str, MessageHandlerType]] = (
        defaultdict(
            dict,
        )
    )
```

### event_type

```python
event_type(message)
```

The event type is determined by the state of the task run

Source code in `fluid/scheduler/consumer.py`

```python
def event_type(self, message: TaskRun) -> str:
    """The event type is determined by the state of the task run"""
    return message.state
```

### register_handler

```python
register_handler(event, handler)
```

Register a handler for the given event

It is possible to register multiple handlers for the same event type by providing a different tag for each handler.

For example, to register two handlers for the event type `foo`:

```python
dispatcher.register_handler("foo.first", handler1)
dispatcher.register_handler("foo.second", handler2)
```

| PARAMETER | DESCRIPTION                                             |
| --------- | ------------------------------------------------------- |
| `event`   | The event to register the handler for **TYPE:** \`Event |
| `handler` | The handler to register **TYPE:** `MessageHandlerType`  |

Source code in `fluid/utils/dispatcher.py`

````python
def register_handler(
    self,
    event: Annotated[Event | str, Doc("The event to register the handler for")],
    handler: Annotated[MessageHandlerType, Doc("The handler to register")],
) -> MessageHandlerType | None:
    """Register a handler for the given event

    It is possible to register multiple handlers for the same event type by
    providing a different tag for each handler.

    For example, to register two handlers for the event type `foo`:

    ```python
    dispatcher.register_handler("foo.first", handler1)
    dispatcher.register_handler("foo.second", handler2)
    ```
    """
    event = Event.from_string_or_event(event)
    previous = self._msg_handlers[event.type].get(event.tag)
    self._msg_handlers[event.type][event.tag] = handler
    return previous
````

### unregister_handler

```python
unregister_handler(event)
```

Unregister a handler for the given event

It returns the handler that was unregistered or `None` if no handler was registered for the given event.

| PARAMETER | DESCRIPTION                                           |
| --------- | ----------------------------------------------------- |
| `event`   | The event to unregister the handler **TYPE:** \`Event |

Source code in `fluid/utils/dispatcher.py`

```python
def unregister_handler(
    self, event: Annotated[Event | str, Doc("The event to unregister the handler")]
) -> MessageHandlerType | None:
    """Unregister a handler for the given event

    It returns the handler that was unregistered or `None` if no handler was
    registered for the given event.
    """
    event = Event.from_string_or_event(event)
    return self._msg_handlers[event.type].pop(event.tag, None)
```

### get_handlers

```python
get_handlers(message)
```

Get all event handlers for the given message

This method returns a dictionary of all handlers registered for the given message type. If no handlers are registered for the message type, it returns `None`.

| PARAMETER | DESCRIPTION                                                 |
| --------- | ----------------------------------------------------------- |
| `message` | The message to get the handlers for **TYPE:** `MessageType` |

Source code in `fluid/utils/dispatcher.py`

```python
def get_handlers(
    self,
    message: Annotated[MessageType, Doc("The message to get the handlers for")],
) -> dict[str, MessageHandlerType] | None:
    """Get all event handlers for the given message

    This method returns a dictionary of all handlers registered for the given
    message type. If no handlers are registered for the message type, it returns
    `None`.
    """
    event_type = self.event_type(message)
    return self._msg_handlers.get(event_type)
```

### dispatch

```python
dispatch(message)
```

dispatch the message to all handlers

It returns the number of handlers that were called

Source code in `fluid/utils/dispatcher.py`

```python
def dispatch(self, message: MessageType) -> int:
    """dispatch the message to all handlers

    It returns the number of handlers that were called
    """
    handlers = self.get_handlers(message)
    if handlers:
        for handler in handlers.values():
            handler(message)
    return len(handlers or ())
```

## fluid.scheduler.task_manager_fastapi

```python
task_manager_fastapi(
    task_manager,
    *,
    app=None,
    include_router=True,
    prefix="/tasks",
    tags=None,
    **kwargs
)
```

Setup the FastAPI app and add the task manager to the state

If the task manager is a Worker, it is also added to the app workers to be started with the app.

| PARAMETER        | DESCRIPTION                                                                                         |
| ---------------- | --------------------------------------------------------------------------------------------------- |
| `task_manager`   | A TaskManager, TaskConsumer or TaskScheduler instance **TYPE:** `TaskManager`                       |
| `app`            | FastAPI app instance. If not provided, a new instance is created. **TYPE:** \`FastAPI               |
| `include_router` | Whether to include the task manager router in the FastAPI app. **TYPE:** `bool` **DEFAULT:** `True` |
| `prefix`         | Prefix for the task manager routes. **TYPE:** `str` **DEFAULT:** `'/tasks'`                         |
| `tags`           | Tags for the task manager routes. **TYPE:** \`Sequence\[str                                         |
| `**kwargs`       | Additional keyword arguments for the FastAPI app if not provided **TYPE:** `Any` **DEFAULT:** `{}`  |

Source code in `fluid/scheduler/endpoints.py`

```python
def task_manager_fastapi(
    task_manager: Annotated[
        TaskManager,
        Doc(
            (
                "A [TaskManager][fluid.scheduler.TaskManager], "
                "[TaskConsumer][fluid.scheduler.TaskConsumer] or "
                "[TaskScheduler][fluid.scheduler.TaskScheduler] instance"
            )
        ),
    ],
    *,
    app: Annotated[
        FastAPI | None,
        Doc("FastAPI app instance. If not provided, a new instance is created."),
    ] = None,
    include_router: Annotated[
        bool,
        Doc("Whether to include the task manager router in the FastAPI app."),
    ] = True,
    prefix: Annotated[
        str,
        Doc("Prefix for the task manager routes."),
    ] = "/tasks",
    tags: Annotated[
        Sequence[str | Enum] | None,
        Doc("Tags for the task manager routes."),
    ] = None,
    **kwargs: Annotated[
        Any,
        Doc("Additional keyword arguments for the FastAPI app if not provided"),
    ],
) -> FastAPI:
    """Setup the FastAPI app and add the task manager to the state

    If the task manager is a [Worker][fluid.utils.worker.Worker], it is also added
    to the app workers to be started with the app.
    """
    app = app or FastAPI(**kwargs)
    if include_router:
        tags_ = tags if tags is not None else ["Tasks"]
        app.include_router(get_router(task_manager), prefix=prefix, tags=list(tags_))
        for plugin in task_manager._plugins:
            plugin.register_routes(app, prefix=prefix, tags=list(tags_))
    app.state.task_manager = task_manager
    if isinstance(task_manager, Worker):
        app_workers(app).add_workers(task_manager)
    else:
        app.router.on_startup.append(task_manager.on_startup)
        app.router.on_shutdown.append(task_manager.on_shutdown)
    return app
```

## fluid.scheduler.endpoints.get_task_manager

```python
get_task_manager(app)
```

Get the task manager added to the app state by task_manager_fastapi.

Use this outside a request, where there is an app but no request to depend on, and TaskManagerDep inside a route.

Source code in `fluid/scheduler/endpoints.py`

```python
def get_task_manager(app: FastAPI) -> TaskManager:
    """Get the task manager added to the app state by
    [task_manager_fastapi][fluid.scheduler.task_manager_fastapi].

    Use this outside a request, where there is an app but no request to depend
    on, and [TaskManagerDep][fluid.scheduler.endpoints.TaskManagerDep] inside a
    route.
    """
    return cast(TaskManager, app.state.task_manager)
```

## fluid.scheduler.endpoints.get_task_manager_from_request

```python
get_task_manager_from_request(request)
```

Get the task manager of the app serving the request.

This is the callable behind TaskManagerDep.

Source code in `fluid/scheduler/endpoints.py`

```python
def get_task_manager_from_request(request: Request) -> TaskManager:
    """Get the task manager of the app serving the request.

    This is the callable behind
    [TaskManagerDep][fluid.scheduler.endpoints.TaskManagerDep].
    """
    return get_task_manager(request.app)
```

## fluid.scheduler.endpoints.TaskManagerDep

```python
TaskManagerDep = TaskManager
```

FastAPI dependency injecting the TaskManager into a route.

Application routes use it to reach the task manager, and through it the dependencies and the resources shared with every task run.
