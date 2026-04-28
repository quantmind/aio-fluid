# Reference

Complete API reference for all public classes, functions, and parameters in Aio Fluid. See the [home page](../index.md) for installation instructions.

## Workers

[Workers](workers.md) — base async worker types with start/stop lifecycle (`Workers`, `WorkerFunction`, `AsyncConsumer`).

## Task Scheduler

- [Task](task.md) — the `@task` decorator, `Task`, `TaskPriority`, `TaskState`, and `K8sConfig`.
- [Task Run](task_run.md) — `TaskRun`, the context object passed to every task executor.
- [Task Retry](task_retry.md) — `RetryPolicy` for failure retries and rate-limit retries.
- [Task Scheduling](task_scheduling.md) — `every()` and `crontab()` schedule helpers.
- [Task Manager](task_manager.md) — `TaskManager`, the base class for running and queuing tasks.
- [Task Consumer](task_consumer.md) — `TaskConsumer`, the worker that dequeues and executes tasks.
- [Task Scheduler](task_scheduler.md) — `TaskScheduler`, combines consumer and scheduler.
- [Task Broker](task_broker.md) — `TaskBroker` interface and the Redis implementation.
- [Task Manager Plugins](task_plugin.md) — extend `TaskManager` with lifecycle hooks.
- [Task Registry](task_registry.md) — internal registry that maps task names to `Task` objects.
- [Task Manager CLI](task_cli.md) — command-line tools for `TaskManager` applications.

## Database

- [Database](db.md) — async Postgres connection and query interface.
- [CrudDB](db_crud.md) — CRUD operations on top of `Database`.
- [DB Migration](db_migrations.md) — schema migration management.
- [DB Pagination](db_pagination.md) — paginated query results.
- [DB CLI](db_cli.md) — command-line tools for database management.

## Utilities

- [Event Dispatchers](dispatchers.md) — `Dispatcher` and `AsyncDispatcher` for decoupled event handling.
- [HTTP Client](http_client.md) — unified async HTTP client wrappers for `aiohttp` and `httpx`.
- [Errors](errors.md) — error hierarchies for utilities and the task scheduler.
- [Utils](utils.md) — miscellaneous helpers.
