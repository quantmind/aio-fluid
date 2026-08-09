# Task Manager Plugins

Plugins extend the TaskManager with additional behaviour by hooking into task lifecycle events.

A plugin implements the TaskManagerPlugin interface and is registered via TaskManager.with_plugin.

```python
from fluid.scheduler import TaskScheduler, task_manager_fastapi
from fluid.scheduler.db import TaskDbPlugin

task_manager = TaskScheduler(...)
task_manager.with_plugin(TaskDbPlugin(db))
app = task_manager_fastapi(task_manager)
```

## fluid.scheduler.TaskManagerPlugin

Bases: `ABC`

Plugin for a task Manager

### register

```python
register(task_manager)
```

Register the plugin with the task manager

Source code in `fluid/scheduler/plugin.py`

```python
@abc.abstractmethod
def register(self, task_manager: TaskManager) -> None:
    """Register the plugin with the task manager"""
```

### register_routes

```python
register_routes(app, prefix='/tasks', tags=None)
```

Register routes with the FastAPI app

| PARAMETER | DESCRIPTION                                                            |
| --------- | ---------------------------------------------------------------------- |
| `app`     | FastAPI app instance. **TYPE:** `FastAPI`                              |
| `prefix`  | The URL prefix for the routes. **TYPE:** `str` **DEFAULT:** `'/tasks'` |
| `tags`    | The tags for the routes. **TYPE:** \`list\[str                         |

Source code in `fluid/scheduler/plugin.py`

```python
def register_routes(  # noqa: B027
    self,
    app: Annotated[
        FastAPI,
        Doc("FastAPI app instance."),
    ],
    prefix: Annotated[
        str,
        Doc("The URL prefix for the routes."),
    ] = "/tasks",
    tags: Annotated[
        list[str | Enum] | None,
        Doc("The tags for the routes."),
    ] = None,
) -> None:
    """Register routes with the FastAPI app"""
```

## fluid.scheduler.db.TaskDbPlugin

```python
TaskDbPlugin(
    db,
    *,
    table_name="fluid_tasks",
    tag="db",
    skip_db_tag="skip_db",
    route_prefix=None
)
```

Bases: `TaskManagerPlugin`

A plugin to store TaskRun in a postgresql database.

This plugin listens to task state changes and updates the database accordingly. It requires a CrudDB instance to perform database operations and allows customization of the table name and event tags.

You can use the `skip_db` tag to prevent database operations for specific tasks.

It can be used if the `db` extra is installed, and requires a compatible database backend supported by CrudDB.

| PARAMETER      | DESCRIPTION                                                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `table_name`   | The name of the table to store task runs **TYPE:** `str` **DEFAULT:** `'fluid_tasks'`                                                      |
| `tag`          | The tag for the plugin event registration **TYPE:** `str` **DEFAULT:** `'db'`                                                              |
| `skip_db_tag`  | The tag to skip database operations **TYPE:** `str` **DEFAULT:** `'skip_db'`                                                               |
| `route_prefix` | Fix the URL prefix for the history routes. If None, routes are registered using the prefix parameter from register_routes. **TYPE:** \`str |

Source code in `fluid/scheduler/db.py`

```python
def __init__(
    self,
    db: CrudDB,
    *,
    table_name: Annotated[
        str,
        Doc("The name of the table to store task runs"),
    ] = "fluid_tasks",
    tag: Annotated[
        str,
        Doc("The tag for the plugin event registration"),
    ] = "db",
    skip_db_tag: Annotated[
        str,
        Doc("The tag to skip database operations"),
    ] = "skip_db",
    route_prefix: Annotated[
        str | None,
        Doc(
            "Fix the URL prefix for the history routes. If None, "
            "routes are registered using the prefix parameter from register_routes."
        ),
    ] = None,
) -> None:
    if table_name not in db.tables:
        task_meta(db.metadata, table_name=table_name)
    self.table_name = table_name
    self.db = db
    self.tag = tag
    self.skip_db_tag = skip_db_tag
    self.route_prefix = route_prefix
```

### table_name

```python
table_name = table_name
```

### db

```python
db = db
```

### tag

```python
tag = tag
```

### skip_db_tag

```python
skip_db_tag = skip_db_tag
```

### route_prefix

```python
route_prefix = route_prefix
```

### register

```python
register(task_manager)
```

Source code in `fluid/scheduler/db.py`

```python
def register(self, task_manager: TaskManager) -> None:
    task_manager.state.task_db_plugin = self
    self.task_manager = task_manager

    if is_in_cpu_process():
        return

    task_manager.register_async_handler(
        Event(TaskState.queued, self.tag),
        self._handle_update,
    )
    task_manager.register_async_handler(
        Event(TaskState.running, self.tag),
        self._handle_update,
    )
    task_manager.register_async_handler(
        Event(TaskState.success, self.tag),
        self._handle_update,
    )
    task_manager.register_async_handler(
        Event(TaskState.failure, self.tag),
        self._handle_update,
    )
    task_manager.register_async_handler(
        Event(TaskState.aborted, self.tag),
        self._handle_update,
    )
    task_manager.register_async_handler(
        Event(TaskState.rate_limited, self.tag),
        self._handle_update,
    )
    task_manager.register_async_handler(
        Event(TaskState.interrupted, self.tag),
        self._handle_update,
    )
```

### register_routes

```python
register_routes(app, prefix='/tasks', tags=None)
```

Register routes with the FastAPI app

| PARAMETER | DESCRIPTION                                                            |
| --------- | ---------------------------------------------------------------------- |
| `app`     | FastAPI app instance. **TYPE:** `FastAPI`                              |
| `prefix`  | The URL prefix for the routes. **TYPE:** `str` **DEFAULT:** `'/tasks'` |
| `tags`    | The tags for the routes. **TYPE:** \`list\[str                         |

Source code in `fluid/scheduler/db.py`

```python
def register_routes(
    self,
    app: Annotated[
        FastAPI,
        Doc("FastAPI app instance."),
    ],
    prefix: Annotated[
        str,
        Doc("The URL prefix for the routes."),
    ] = "/tasks",
    tags: Annotated[
        list[str | Enum] | None,
        Doc("The tags for the routes."),
    ] = None,
) -> None:
    """Register routes with the FastAPI app"""
    prefix = self.route_prefix or f"{prefix}-history"
    app.include_router(router, prefix=prefix, tags=tags)
```

### get_history

```python
get_history(q)
```

Get task run history based on the provided query parameters.

| PARAMETER | DESCRIPTION                                                                 |
| --------- | --------------------------------------------------------------------------- |
| `q`       | Query parameters for fetching task run history **TYPE:** `TaskHistoryQuery` |

Source code in `fluid/scheduler/db.py`

```python
async def get_history(
    self,
    q: Annotated[
        TaskHistoryQuery, Doc("Query parameters for fetching task run history")
    ],
) -> TaskRunHistoryPage:
    """Get task run history based on the provided query parameters."""
    table = self.db.tables[self.table_name]
    filters = q.filters()
    if q.tags:
        wanted = set(q.tags)
        names = {
            name
            for name, task in self.task_manager.registry.items()
            if wanted & task.tags
        }
        # AND with an explicit task filter; empty set → IN () → no rows
        if "name" in filters:
            names &= {filters["name"]}
        filters["name"] = list(names)
    pagination = Pagination.create(
        "queued",
        filters=filters,
        limit=q.limit,
        cursor=q.cursor,
        desc=True,
    )
    rows, cursor = await pagination.execute(self.db, table)
    return TaskRunHistoryPage(
        data=[_row_to_task_run(row) for row in rows],
        cursor=cursor,
    )
```

### get_run

```python
get_run(run_id)
```

Get a specific task run by its ID.

Source code in `fluid/scheduler/db.py`

```python
async def get_run(self, run_id: str) -> TaskRunHistory:
    """Get a specific task run by its ID."""
    table = self.db.tables[self.table_name]
    result = await self.db.db_select(table, {"id": run_id})
    rows = result.fetchall()
    if not rows:
        raise NoResultFound(f"Task run with id {run_id} not found")
    return _row_to_task_run(rows[0])
```

## Accessing the plugin from a task

get_db_plugin retrieves the registered TaskDbPlugin from the task manager state. It is designed as a FastAPI dependency for route handlers, but can also be called directly from within a task by passing `context.task_manager`:

```python
from fluid.scheduler import TaskRun, task
from fluid.scheduler.db import get_db_plugin, TaskHistoryQuery


@task()
async def report(context: TaskRun) -> None:
    db_plugin = get_db_plugin(context.task_manager)
    page = await db_plugin.get_history(TaskHistoryQuery(task="my-task", limit=10))
    for run in page.data:
        print(run.id, run.state)
```

## fluid.scheduler.db.get_db_plugin

```python
get_db_plugin(task_manager)
```

Retrieve the registered TaskDbPlugin.

Can be used as a FastAPI dependency in route handlers, or called directly from within a task by passing `context.task_manager`.

Source code in `fluid/scheduler/db.py`

```python
def get_db_plugin(task_manager: TaskManagerDep) -> TaskDbPlugin:
    """Retrieve the registered [TaskDbPlugin][fluid.scheduler.db.TaskDbPlugin].

    Can be used as a FastAPI dependency in route handlers, or called directly
    from within a task by passing `context.task_manager`.
    """
    return task_manager.state.task_db_plugin
```

## History Models

The following models are used when querying task run history via TaskDbPlugin.get_history or the HTTP endpoints.

They can be imported from `fluid.scheduler.db`:

```python
from fluid.scheduler.db import TaskHistoryQuery, TaskRunHistory, TaskRunHistoryPage
```

### Filtering by tags

The `tags` field of TaskHistoryQuery filters runs by the tags of their Task. A run matches when its task carries **at least one** of the supplied tags (OR semantics, the same as the `tags` query parameter on the task list endpoint). Tags are resolved against the live task registry at query time, so they always reflect each task's *current* tags rather than the tags it had when the run executed.

When combined with the `task` filter, the two are applied together (AND): the run's task must match the name *and* carry one of the tags. Tags that match no registered task return an empty result.

## fluid.scheduler.db.TaskHistoryQuery

Bases: `BaseModel`

Query parameters for fetching task run history.

Fields:

- `task` (`str | None`)
- `start` (`datetime | None`)
- `end` (`datetime | None`)
- `state` (`TaskState | None`)
- `params` (`dict[str, Any] | str | None`)
- `tags` (`list[str] | None`)
- `limit` (`int | None`)
- `cursor` (`str`)

Validators:

- `_parse_params_str`

### task

```python
task = None
```

Filter by task name when provided

### start

```python
start = None
```

Filter runs queued at or after this time when provided

### end

```python
end = None
```

Filter runs queued at or before this time when provided

### state

```python
state = None
```

Filter by task state when provided

### params

```python
params = None
```

Filter by params using JSON containment when provided

### tags

```python
tags = None
```

Filter runs whose task has at least one of these tags when provided. Tags are resolved against the live task registry, so they reflect each task's current tags.

### limit

```python
limit = None
```

Maximum number of results to return when provided

### cursor

```python
cursor = ''
```

Pagination cursor from a previous response when provided

### filters

```python
filters()
```

Source code in `fluid/scheduler/db.py`

```python
def filters(self) -> dict:
    return {
        self._filter_map.get(k, k): v
        for k, v in self.model_dump(
            exclude_none=True, exclude={"limit", "cursor", "tags"}
        ).items()
    }
```

## fluid.scheduler.db.TaskRunHistory

Bases: `BaseModel`

A model representing the history of a task run, including its parameters and timing information.

Fields:

- `id` (`str`)
- `task` (`str`)
- `priority` (`TaskPriority`)
- `state` (`TaskState`)
- `queued` (`datetime`)
- `start` (`datetime | None`)
- `end` (`datetime | None`)
- `params` (`dict[str, Any]`)

### id

```python
id
```

The unique ID of the task run

### task

```python
task
```

The name of the task

### priority

```python
priority
```

The priority of the task

### state

```python
state
```

The state of the task

### queued

```python
queued
```

The time the task was queued

### start

```python
start = None
```

The start time of the task

### end

```python
end = None
```

The end time of the task

### params

```python
params
```

The parameters of the task run

## fluid.scheduler.db.TaskRunHistoryPage

Bases: `BaseModel`

A paginated response containing a list of task run history records.

Returned by TaskDbPlugin.get_history and the `GET /task-history` endpoint.

Fields:

- `data` (`list[TaskRunHistory]`)
- `cursor` (`str`)

### data

```python
data
```

The task run history records

### cursor

```python
cursor
```

Pagination cursor to fetch the next page
