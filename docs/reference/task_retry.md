# Task Retry

`aio-fluid` supports automatic retries for two distinct failure modes: execution failures and rate limiting.
Both are configured per-task via [RetryPolicy][fluid.scheduler.models.RetryPolicy] objects passed to the [@task][fluid.scheduler.task] decorator.

```python
from fluid.scheduler import RetryPolicy, task, TaskRun
```

## RetryPolicy

::: fluid.scheduler.models.RetryPolicy

## Configuring retries on a task

### Failure retry

Set `retry` on [@task][fluid.scheduler.task] to re-queue the task when its executor raises an exception.
The [TaskRun][fluid.scheduler.TaskRun] is re-queued with an `execute_after` delay computed from the [RetryPolicy][fluid.scheduler.models.RetryPolicy]; the worker that dequeued it is freed immediately to process other tasks.

```python
from fluid.scheduler import RetryPolicy, task, TaskRun

@task(retry=RetryPolicy(max_attempts=3, wait=2.0, backoff=2.0))
async def fetch(ctx: TaskRun) -> None:
    ...
```

With `backoff=2.0` the delays between attempts are `2s → 4s → 8s`.
Use `backoff=1.0` (the default) for a fixed delay.

To retry only on specific exception types, pass `exceptions`:

```python
@task(retry=RetryPolicy(max_attempts=5, wait=1.0, exceptions=(IOError, TimeoutError)))
async def fetch(ctx: TaskRun) -> None:
    ...
```

### Rate-limit retry

Set `rate_limit_retry` on [@task][fluid.scheduler.task] to re-queue the task when it cannot start because
[max_concurrency][fluid.scheduler.Task.max_concurrency] is already reached.
Without this policy, the [TaskRun][fluid.scheduler.TaskRun] ends immediately in the [rate_limited][fluid.scheduler.TaskState] state.

```python
@task(
    max_concurrency=1,
    rate_limit_retry=RetryPolicy(max_attempts=5, wait=10.0, backoff=1.5, max_wait=120.0),
)
async def exclusive(ctx: TaskRun) -> None:
    ...
```

### How re-queuing works

Both retry modes share the same mechanism:

1. The [TaskConsumer][fluid.scheduler.TaskConsumer] detects the failure (execution error or concurrency limit).
2. It creates a copy of the [TaskRun][fluid.scheduler.TaskRun] with a fresh state and an `execute_after` timestamp set to `now + delay`.
3. The copy is pushed back onto the Redis queue via the [TaskBroker][fluid.scheduler.TaskBroker].
4. The [TaskConsumer][fluid.scheduler.TaskConsumer] is freed immediately — no sleeping.
5. When the copy is next dequeued, if `execute_after` is still in the future it is re-scheduled via `call_later` and the worker moves on; otherwise execution proceeds normally.

!!! note
    The minimum effective re-queue delay is **5 seconds**, regardless of the `wait` value in the policy.
    A `call_later` is used to avoid busy-looping, and the floor ensures the worker is not called back too aggressively.
