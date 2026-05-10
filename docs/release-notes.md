# Release Notes

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
