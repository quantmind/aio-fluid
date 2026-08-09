# Reference

Complete API reference for all public classes, functions, and parameters in Aio Fluid. See the [home page](https://fluid.quantmind.com/index.md) for installation instructions.

## Workers

[Workers](https://fluid.quantmind.com/reference/workers/index.md) — base async worker types with start/stop lifecycle (`Workers`, `WorkerFunction`, `AsyncConsumer`).

## Task Scheduler

- [Task](https://fluid.quantmind.com/reference/task/index.md) — the `@task` decorator, `Task`, `TaskPriority`, `TaskState`, and `K8sConfig`.
- [Task Run](https://fluid.quantmind.com/reference/task_run/index.md) — `TaskRun`, the context object passed to every task executor.
- [Task Retry](https://fluid.quantmind.com/reference/task_retry/index.md) — `RetryPolicy` for failure retries and rate-limit retries.
- [Task Scheduling](https://fluid.quantmind.com/reference/task_scheduling/index.md) — `every()` and `crontab()` schedule helpers.
- [Task Manager](https://fluid.quantmind.com/reference/task_manager/index.md) — `TaskManager`, the base class for running and queuing tasks.
- [Task Consumer](https://fluid.quantmind.com/reference/task_consumer/index.md) — `TaskConsumer`, the worker that dequeues and executes tasks.
- [Task Scheduler](https://fluid.quantmind.com/reference/task_scheduler/index.md) — `TaskScheduler`, combines consumer and scheduler.
- [Task Broker](https://fluid.quantmind.com/reference/task_broker/index.md) — `TaskBroker` interface and the Redis implementation.
- [Task Manager Plugins](https://fluid.quantmind.com/reference/task_plugin/index.md) — extend `TaskManager` with lifecycle hooks.
- [Task Registry](https://fluid.quantmind.com/reference/task_registry/index.md) — internal registry that maps task names to `Task` objects.
- [Task Manager CLI](https://fluid.quantmind.com/reference/task_cli/index.md) — command-line tools for `TaskManager` applications.

## Database

- [Database](https://fluid.quantmind.com/reference/db/index.md) — async Postgres connection and query interface.
- [CrudDB](https://fluid.quantmind.com/reference/db_crud/index.md) — CRUD operations on top of `Database`.
- [DB Migration](https://fluid.quantmind.com/reference/db_migrations/index.md) — schema migration management.
- [DB Pagination](https://fluid.quantmind.com/reference/db_pagination/index.md) — paginated query results.
- [DB CLI](https://fluid.quantmind.com/reference/db_cli/index.md) — command-line tools for database management.

## Utilities

- [Event Dispatchers](https://fluid.quantmind.com/reference/dispatchers/index.md) — `Dispatcher` and `AsyncDispatcher` for decoupled event handling.
- [HTTP Client](https://fluid.quantmind.com/reference/http_client/index.md) — unified async HTTP client wrappers for `aiohttp` and `httpx`.
- [Errors](https://fluid.quantmind.com/reference/errors/index.md) — error hierarchies for utilities and the task scheduler.
- [Utils](https://fluid.quantmind.com/reference/utils/index.md) — miscellaneous helpers.
