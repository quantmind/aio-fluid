# Release Notes

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
