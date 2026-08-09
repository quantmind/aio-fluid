# Task Retry

`aio-fluid` supports automatic retries for two distinct failure modes: execution failures and rate limiting. Both are configured per-task via RetryPolicy objects passed to the @task decorator.

```python
from fluid.scheduler import RetryPolicy, task, TaskRun
```

## RetryPolicy

## fluid.scheduler.models.RetryPolicy

```python
RetryPolicy(
    max_attempts=None,
    wait=1.0,
    backoff=1.0,
    max_wait=60.0,
    exceptions=(),
)
```

Retry policy for task execution failures.

```python
from fluid.scheduler import RetryPolicy, task

@task(retry=RetryPolicy(max_attempts=3, wait=2.0, backoff=2.0))
async def my_task(ctx: TaskRun) -> None:
    ...
```

### max_attempts

```python
max_attempts = None
```

Maximum number of retry attempts, not counting the initial attempt. If None, there is no limit on the number of attempts.

### wait

```python
wait = 1.0
```

Base wait time in seconds before the first retry.

### backoff

```python
backoff = 1.0
```

Multiplier applied to `wait` on each successive attempt. Use `1.0` for fixed delay, `2.0` for exponential backoff.

### max_wait

```python
max_wait = 60.0
```

Upper bound on wait time in seconds regardless of backoff.

### exceptions

```python
exceptions = ()
```

Exception types that trigger a retry. Empty tuple matches all exceptions.

### delay

```python
delay(attempt)
```

Compute wait time before the given attempt number (1-based).

Source code in `fluid/scheduler/models.py`

```python
def delay(self, attempt: int) -> float:
    """Compute wait time before the given attempt number (1-based)."""
    return min(self.wait * (self.backoff ** (attempt - 1)), self.max_wait)
```

### matches

```python
matches(exc)
```

Return True if this exception should trigger a retry.

Source code in `fluid/scheduler/models.py`

```python
def matches(self, exc: Exception) -> bool:
    """Return True if this exception should trigger a retry."""
    if not self.exceptions:
        return True
    return isinstance(exc, self.exceptions)
```

## Configuring retries on a task

### Failure retry

Set `retry` on @task to re-queue the task when its executor raises an exception. The TaskRun is re-queued with an `execute_after` delay computed from the RetryPolicy; the worker that dequeued it is freed immediately to process other tasks.

```python
from fluid.scheduler import RetryPolicy, task, TaskRun

@task(retry=RetryPolicy(max_attempts=3, wait=2.0, backoff=2.0))
async def fetch(ctx: TaskRun) -> None:
    ...
```

With `backoff=2.0` the delays between attempts are `2s → 4s → 8s`. Use `backoff=1.0` (the default) for a fixed delay.

To retry only on specific exception types, pass `exceptions`:

```python
@task(retry=RetryPolicy(max_attempts=5, wait=1.0, exceptions=(IOError, TimeoutError)))
async def fetch(ctx: TaskRun) -> None:
    ...
```

### Rate-limit retry

Set `rate_limit_retry` on @task to re-queue the task when it cannot start because max_concurrency is already reached. Without this policy, the TaskRun ends immediately in the rate_limited state.

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

1. The TaskConsumer detects the failure (execution error or concurrency limit).
1. It creates a copy of the TaskRun with a fresh state and an `execute_after` timestamp set to `now + delay`.
1. The copy is pushed back onto the Redis queue via the TaskBroker.
1. The TaskConsumer is freed immediately — no sleeping.
1. When the copy is next dequeued, if `execute_after` is still in the future it is re-scheduled via `call_later` and the worker moves on; otherwise execution proceeds normally.

Note

The minimum effective re-queue delay is **5 seconds**, regardless of the `wait` value in the policy. A `call_later` is used to avoid busy-looping, and the floor ensures the worker is not called back too aggressively.
