# Release Notes

This page is the source of truth for aio-fluid release notes. Each section below maps to a tagged release on [GitHub](https://github.com/quantmind/aio-fluid/releases). When a new tag is pushed, the matching section is extracted by `.github/workflows/release.yml` and published as the GitHub Release body.

## v2.6.0

An application that aliases the task run to bind its dependencies keeps the parameters of its tasks. Python 3.11 is no longer supported: see **Breaking changes** below.

### Breaking changes

- Python 3.11 is no longer supported, the minimum version is now 3.12. The alias resolution below relies on `typing.TypeAliasType`, which the standard library only exposes from 3.12.

### Improvements and fixes

- A task annotated with a type alias of the task run keeps its parameters model. An application binding its dependencies once, as in `type AppTaskRun[P] = TaskRun[P, AppDeps]`, and annotating a task with `AppTaskRun[Params]` used to silently get [EmptyParams](https://fluid.quantmind.com/reference/task_run/): an alias is not a class, so it did not survive the inspection of the annotation, and the parameters passed when queueing the task were dropped. The alias is now resolved to the type it stands for before the annotation is read, both bare and subscripted.

## v2.5.0

Task runs can queue other task runs and the chain is recorded on every run, CPU bound tasks behave the same way in a subprocess as on the event loop, and Kubernetes Jobs inherit the task timeout. The documentation gained a settings reference, a recipes cheat sheet and a page on pointing coding agents at the library.

- A task run can queue another task run with [TaskRun.queue](https://fluid.quantmind.com/reference/task_run/), which records the queueing run in `from_run_id` and carries the `root_run_id` of the first run in the chain, so any run can be traced back to the one that started it. ([#110](https://github.com/quantmind/aio-fluid/pull/110))
- CPU bound tasks now start the async event dispatcher in the subprocess that runs them, so lifecycle events reach the handlers and plugins there as well, and a consumer with CPU bound tasks that is not started from [TaskManagerCLI](https://fluid.quantmind.com/reference/task_cli/) raises the new `CpuBoundEntryPointError` on startup rather than failing when the first such task runs.
- A Kubernetes Job created for a CPU bound task sets `active_deadline_seconds` from the task `timeout_seconds`, so the cluster terminates a Job that overruns. ([#108](https://github.com/quantmind/aio-fluid/pull/108))
- New [Settings](https://fluid.quantmind.com/reference/settings/) reference page covering the environment variables that configure the task consumer, broker, database and HTTP client, including the prefix rules and the fields that keep an unprefixed name.
- Fixed the [Task Broker](https://fluid.quantmind.com/tutorials/task_broker/) tutorial, which imported a name that does not exist, quoted the wrong default Redis port, and listed six outdated abstract methods instead of the sixteen a broker has to implement.
- Admonition blocks are rendered as admonitions instead of literal text, which also fixes the note on retry delays in the [Task Retry](https://fluid.quantmind.com/reference/task_retry/) reference.
- New [recipes](https://fluid.quantmind.com/recipes/) cheat sheet, a page on [using the docs with AI agents](https://fluid.quantmind.com/ai-agents/) and an `AGENTS.md` for contributors. ([#111](https://github.com/quantmind/aio-fluid/pull/111))
- New tutorials on [task dependencies](https://fluid.quantmind.com/tutorials/task_deps/), [choosing a task manager](https://fluid.quantmind.com/tutorials/task_managers/) and [extending the FastAPI app](https://fluid.quantmind.com/tutorials/task_fastapi/). ([#109](https://github.com/quantmind/aio-fluid/pull/109), [#111](https://github.com/quantmind/aio-fluid/pull/111))
- New [comparison](https://fluid.quantmind.com/comparison/) page placing the library next to Celery, RQ, arq and taskiq, with download numbers refreshed by a scheduled workflow, and a landing page rewritten around CPU bound work. ([#104](https://github.com/quantmind/aio-fluid/pull/104), [#105](https://github.com/quantmind/aio-fluid/pull/105), [#107](https://github.com/quantmind/aio-fluid/pull/107))

## v2.4.3

Fixes two task queue issues: pydantic secret params were masked when a task run was serialized to the queue, and a params validation error on the consumer side crashed the worker.

- Secret params now survive the round-trip through the task queue and the cpu-bound subprocess. Task runs are serialized for the queue with secret values revealed via the new `params_dump` helper; all other dumps (logs, endpoints) keep secrets masked. ([#103](https://github.com/quantmind/aio-fluid/pull/103))
- A task run consumed from the queue with invalid params no longer kills the consumer worker. The broker raises the new `TaskParamsError` carrying the task run, and the consumer logs the error and marks the run as failed. ([#103](https://github.com/quantmind/aio-fluid/pull/103))

## v2.4.2

Tasks can now be tagged at registration time.

- The task registration methods accept an optional `tags` argument. When provided, the extra tags are merged into each task's own tags as it is registered — applied across `register_task`, `register_from_module`, and `register_from_dict`. ([#102](https://github.com/quantmind/aio-fluid/pull/102))
- Bumped `python-json-logger` to `>= 4.1.0` and switched JSON logging to the new `pythonjsonlogger.json` formatter path (the old `pythonjsonlogger.jsonlogger` module is deprecated). ([#102](https://github.com/quantmind/aio-fluid/pull/102))

## v2.4.1

Task history can now be filtered by task tags.

- Added a `tags` field to task history queries. Runs match when their task carries at least one of the given tags, resolved against the live registry. ([#101](https://github.com/quantmind/aio-fluid/pull/101))

## v2.4.0

Lazy settings via pydantic-settings, JSONB params filtering for task history, and customisable route prefixes.

- Settings are now lazy — resolved on first access instead of at import time. Env vars use a `FLUID_` prefix by default; legacy unprefixed names are kept as aliases. ([#100](https://github.com/quantmind/aio-fluid/pull/100))
- The task database plugin accepts a `route_prefix` parameter for customising history route URLs and replaces `with_task_history_router()` with a `register_routes()` method. ([#100](https://github.com/quantmind/aio-fluid/pull/100))
- Task history queries support filtering by run params via a new `params` field (renamed from `HistoryQuery` to `TaskHistoryQuery`). ([#99](https://github.com/quantmind/aio-fluid/pull/99))
- **Database migration required:** the `params` column is now `JSONB` with a GIN index. See the example [migration](https://github.com/quantmind/aio-fluid/blob/main/examples/tasks/migrations/versions/d941c11ca25a_jsonb.py) for the schema changes.
- Removed `get_logger` from `fluid.utils.log`. Task loggers are now obtained directly via `logging.getLogger(module)`.

## v2.3.1

**v2.3.0 is broken — do not use it.**

Fixes a regression in v2.3.0 where the `httpx2` dependency was pinned to `>=2.2.0`, which fails on Python 3.14 builds missing the `_zstd` C extension. Pins `httpx2` to `>=2.0.0, <2.1.0` and switches all `httpx` imports to `httpx2` for correct namespace resolution.

- `httpx2` is now pinned to `>=2.0.0, <2.1.0` — versions 2.1.0+ require the `compression.zstd` stdlib module which is not available in all Python 3.14 builds. ([#98](https://github.com/quantmind/aio-fluid/pull/98))
- All `import httpx` statements replaced with `import httpx2 as httpx` (or `from httpx2 import ...`) to ensure correct namespace resolution regardless of `httpx2` version.
- Added test coverage for [HttpxClient](https://fluid.quantmind.com/reference/http_client/#fluid.utils.http_client.HttpxClient) and [HttpxResponse](https://fluid.quantmind.com/reference/http_client/#fluid.utils.http_client.HttpxResponse).

## v2.3.0

Moves development and documentation dependencies from optional-dependencies to [dependency groups](https://peps.python.org/pep-0735/), switches to [httpx2](https://pypi.org/project/httpx2/) for HTTP client support, and removes the `inflection` dependency.

- `dev` and `docs` dependencies are now declared under `[dependency-groups]` instead of `[project.optional-dependencies]`. Installed via `uv sync --all-groups`.
- The `http` extra now uses `httpx2` instead of `httpx`. `httpx2` provides the same `httpx` module so no code changes are required. ([#97](https://github.com/quantmind/aio-fluid/pull/97))
- The `inflection` dependency has been removed. ([#96](https://github.com/quantmind/aio-fluid/pull/96))

## v2.2.6

Adds tag filtering for task listings and fixes a race in the task database plugin.

- Task listings can be filtered by tag: the `GET /tasks` endpoint and the `ls` command of the [task CLI](https://fluid.quantmind.com/reference/task_cli/) accept a repeatable `tags` option that returns only tasks carrying at least one of the given tags, and `TaskInfo` now reports each task's tags. ([#94](https://github.com/quantmind/aio-fluid/pull/94))
- The [task database plugin](https://fluid.quantmind.com/reference/task_plugin/#fluid.scheduler.db.TaskDbPlugin) now serialises its per-run lifecycle writes with a dedicated task-run lock. `CrudDB.db_upsert` is not atomic — it issues an `UPDATE` and only `INSERT`s when nothing matched — so when the scheduler wrote the `queued` row and a consumer wrote the `running` row a few milliseconds later, the consumer's `UPDATE` could miss the not-yet-committed `INSERT`, fall through to its own `INSERT` and violate the task-runs primary key. Holding the lock around the upsert removes the race. ([#95](https://github.com/quantmind/aio-fluid/pull/95))
- [TaskRun.lock](https://fluid.quantmind.com/reference/task_run/#fluid.scheduler.TaskRun.lock) accepts an optional `name` to acquire a named sub-lock for the task run, and `timeout` now defaults to `None`. ([#95](https://github.com/quantmind/aio-fluid/pull/95))

## v2.2.5

- The [task decorator](https://fluid.quantmind.com/reference/task/#fluid.scheduler.task) accepts an `env` mapping of extra environment variables, injected into the subprocess for CPU-bound tasks and forwarded to the container for tasks dispatched as Kubernetes Jobs. ([#90](https://github.com/quantmind/aio-fluid/pull/90))

## v2.2.4

Bug-fix release for the task scheduler.

- Fix task interruption handling. ([#89](https://github.com/quantmind/aio-fluid/pull/89))
- Fix stale concurrent tasks not being released. ([#88](https://github.com/quantmind/aio-fluid/pull/88))
- Fix task abort behaviour. ([#87](https://github.com/quantmind/aio-fluid/pull/87))
- Patch `pydanclick` to work with `StrEnum`. ([#86](https://github.com/quantmind/aio-fluid/pull/86))
