# Task Retries

Tasks can be configured to retry automatically when they fail or when they cannot run due to concurrency limits.
Both behaviours are controlled by a [RetryPolicy][fluid.scheduler.models.RetryPolicy] attached to the task via the [@task][fluid.scheduler.task] decorator.

## Retrying on failure

Use the `retry` parameter to re-queue a task after an execution error.

```python
from fluid.scheduler import RetryPolicy, task, TaskRun

@task(retry=RetryPolicy(max_attempts=3, wait=2.0, backoff=2.0))
async def fetch_data(ctx: TaskRun) -> None:
    """Fetch data from an external API."""
    response = await ctx.deps.http_client.get("https://api.example.com/data")
    ctx.logger.info("fetched %d bytes", len(response))
```

If `fetch_data` raises, the [TaskConsumer][fluid.scheduler.TaskConsumer] re-queues it with a delay and moves on.
With `backoff=2.0` the wait times grow exponentially: `2s → 4s → 8s`.
After 3 failed retries the [TaskRun][fluid.scheduler.TaskRun] ends in the [failure][fluid.scheduler.TaskState] state.

### Limiting which exceptions trigger a retry

By default all exceptions trigger a retry. Pass `exceptions` to be more selective:

```python
@task(retry=RetryPolicy(max_attempts=5, wait=1.0, exceptions=(IOError, TimeoutError)))
async def fetch_data(ctx: TaskRun) -> None:
    ...
```

`ValueError` or other programming errors will not be retried and the task fails immediately.

## Retrying when rate limited

When [max_concurrency][fluid.scheduler.Task.max_concurrency] is set, a task that cannot start because the limit is already reached ends in the [rate_limited][fluid.scheduler.TaskState] state by default.
Set `rate_limit_retry` to re-queue it instead:

```python
@task(
    max_concurrency=1,
    rate_limit_retry=RetryPolicy(max_attempts=10, wait=5.0),
)
async def exclusive_job(ctx: TaskRun) -> None:
    """Only one instance of this task can run at a time."""
    ...
```

The task will be re-queued up to 10 times, waiting 5 seconds between each attempt.
If the slot is still occupied after all attempts, the run ends in [rate_limited][fluid.scheduler.TaskState].

## Combining both policies

A task can have both policies simultaneously:

```python
@task(
    max_concurrency=2,
    retry=RetryPolicy(max_attempts=3, wait=2.0, backoff=2.0, max_wait=30.0),
    rate_limit_retry=RetryPolicy(max_attempts=5, wait=10.0),
)
async def resilient_task(ctx: TaskRun) -> None:
    ...
```

## How retries work under the hood

Both retry paths use the same mechanism — no workers are blocked waiting:

1. The [TaskConsumer][fluid.scheduler.TaskConsumer] detects the failure or concurrency limit.
2. It creates a fresh copy of the [TaskRun][fluid.scheduler.TaskRun] with `execute_after` set to `now + delay`.
3. The copy is pushed back onto the Redis queue via the [TaskBroker][fluid.scheduler.TaskBroker] and the worker moves on immediately.
4. When a worker dequeues the copy and `execute_after` is still in the future, it schedules
   re-queuing via `call_later` and returns — still without blocking.
5. Once the delay has elapsed the task enters the queue normally and is executed.
