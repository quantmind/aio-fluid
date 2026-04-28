# Tutorials

Step-by-step guides for using Aio Fluid in your backend applications. See the [home page](../index.md) for installation instructions.

## Workers

[Workers](workers.md) are the foundational building block — async components with start/stop lifecycle management. Start here if you are new to the library.

## Task Queue

- [Tasks](task_queue.md) — define tasks with the `@task` decorator, set priorities, scheduling, concurrency limits, and CPU-bound execution.
- [Task Queue App](task_app.md) — wire tasks into a full producer/consumer application with FastAPI and Redis.
- [Task Broker](task_broker.md) — implement a custom broker to replace the default Redis backend.
- [Task Retries](task_retry.md) — automatically retry tasks on failure or when rate-limited by concurrency.
- [K8s Jobs](task_k8s.md) — dispatch CPU-bound tasks as Kubernetes Jobs instead of local subprocesses.

## Database

[Async Database](db.md) — CRUD operations and migrations for Postgres using SQLAlchemy and asyncpg.

## Utilities

[Event Dispatchers](dispatchers.md) — decouple event sources from handlers for flexible async pipelines.
