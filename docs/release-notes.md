# Release Notes

## v2.4.1

Task history can now be filtered by task tags.

- Added a `tags` field to task history queries. Runs match when their task
  carries at least one of the given tags, resolved against the live registry.
  ([#101](https://github.com/quantmind/aio-fluid/pull/101))

## v2.4.0

Lazy settings via pydantic-settings, JSONB params filtering for task
history, and customisable route prefixes.

- Settings are now lazy — resolved on first access instead of at import
  time. Env vars use a `FLUID_` prefix by default; legacy unprefixed names
  are kept as aliases.
  ([#100](https://github.com/quantmind/aio-fluid/pull/100))
- The task database plugin accepts a `route_prefix` parameter for
  customising history route URLs and replaces `with_task_history_router()`
  with a `register_routes()` method.
  ([#100](https://github.com/quantmind/aio-fluid/pull/100))
- Task history queries support filtering by run params via a new `params`
  field (renamed from `HistoryQuery` to `TaskHistoryQuery`).
  ([#99](https://github.com/quantmind/aio-fluid/pull/99))
- **Database migration required:** the `params` column is now `JSONB` with
  a GIN index. See the example
  [migration](https://github.com/quantmind/aio-fluid/blob/main/examples/tasks/migrations/versions/d941c11ca25a_jsonb.py)
  for the schema changes.
- Removed `get_logger` from `fluid.utils.log`. Task loggers are now obtained
  directly via `logging.getLogger(module)`.

## v2.3.1

**v2.3.0 is broken — do not use it.**

Fixes a regression in v2.3.0 where the `httpx2` dependency was pinned to
`>=2.2.0`, which fails on Python 3.14 builds missing the `_zstd` C extension.
Pins `httpx2` to `>=2.0.0, <2.1.0` and switches all `httpx` imports to
`httpx2` for correct namespace resolution.

- `httpx2` is now pinned to `>=2.0.0, <2.1.0` — versions 2.1.0+ require the
  `compression.zstd` stdlib module which is not available in all Python 3.14
  builds.
  ([#98](https://github.com/quantmind/aio-fluid/pull/98))
- All `import httpx` statements replaced with `import httpx2 as httpx` (or
  `from httpx2 import ...`) to ensure correct namespace resolution regardless
  of `httpx2` version.
- Added test coverage for [HttpxClient](https://fluid.quantmind.com/reference/http_client/#fluid.utils.http_client.HttpxClient)
  and [HttpxResponse](https://fluid.quantmind.com/reference/http_client/#fluid.utils.http_client.HttpxResponse).

## v2.3.0

Moves development and documentation dependencies from optional-dependencies to
[dependency groups](https://peps.python.org/pep-0735/), switches to
[httpx2](https://pypi.org/project/httpx2/) for HTTP client support, and removes
the `inflection` dependency.

- `dev` and `docs` dependencies are now declared under `[dependency-groups]`
  instead of `[project.optional-dependencies]`. Installed via `uv sync
  --all-groups`.
- The `http` extra now uses `httpx2` instead of `httpx`. `httpx2` provides the
  same `httpx` module so no code changes are required.
  ([#97](https://github.com/quantmind/aio-fluid/pull/97))
- The `inflection` dependency has been removed.
  ([#96](https://github.com/quantmind/aio-fluid/pull/96))

## v2.2.6

Adds tag filtering for task listings and fixes a race in the task database plugin.

- Task listings can be filtered by tag: the `GET /tasks` endpoint and the `ls`
  command of the [task CLI](https://fluid.quantmind.com/reference/task_cli/)
  accept a repeatable `tags` option that returns only tasks carrying at least
  one of the given tags, and `TaskInfo` now reports each task's tags.
  ([#94](https://github.com/quantmind/aio-fluid/pull/94))
- The [task database plugin](https://fluid.quantmind.com/reference/task_plugin/#fluid.scheduler.db.TaskDbPlugin)
  now serialises its per-run lifecycle writes with a dedicated task-run lock.
  `CrudDB.db_upsert` is not atomic — it issues an `UPDATE` and only `INSERT`s
  when nothing matched — so when the scheduler wrote the `queued` row and a
  consumer wrote the `running` row a few milliseconds later, the consumer's
  `UPDATE` could miss the not-yet-committed `INSERT`, fall through to its own
  `INSERT` and violate the task-runs primary key. Holding the lock around the
  upsert removes the race.
  ([#95](https://github.com/quantmind/aio-fluid/pull/95))
- [TaskRun.lock](https://fluid.quantmind.com/reference/task_run/#fluid.scheduler.TaskRun.lock)
  accepts an optional `name` to acquire a named sub-lock for the task run, and
  `timeout` now defaults to `None`. ([#95](https://github.com/quantmind/aio-fluid/pull/95))

## v2.2.5

- The [task decorator](https://fluid.quantmind.com/reference/task/#fluid.scheduler.task)
  accepts an `env` mapping of extra environment variables, injected into the
  subprocess for CPU-bound tasks and forwarded to the container for tasks
  dispatched as Kubernetes Jobs.
  ([#90](https://github.com/quantmind/aio-fluid/pull/90))

## v2.2.4

Bug-fix release for the task scheduler.

- Fix task interruption handling. ([#89](https://github.com/quantmind/aio-fluid/pull/89))
- Fix stale concurrent tasks not being released. ([#88](https://github.com/quantmind/aio-fluid/pull/88))
- Fix task abort behaviour. ([#87](https://github.com/quantmind/aio-fluid/pull/87))
- Patch `pydanclick` to work with `StrEnum`. ([#86](https://github.com/quantmind/aio-fluid/pull/86))
