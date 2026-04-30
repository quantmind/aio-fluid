from datetime import timedelta
from time import monotonic

import pytest

from fluid.scheduler import (
    RetryPolicy,
    TaskPriority,
    TaskRun,
    TaskScheduler,
    TaskState,
    task,
)
from fluid.scheduler.consumer import InProcessTaskQueue
from fluid.scheduler.errors import TaskRunError
from fluid.scheduler.models import TaskDecoratorError
from fluid.utils.dates import utcnow
from fluid.utils.text import create_uid
from fluid.utils.waiter import wait_for

pytestmark = pytest.mark.asyncio(loop_scope="module")


# ---------------------------------------------------------------------------
# RetryPolicy unit tests
# ---------------------------------------------------------------------------


async def test_delay_fixed():
    policy = RetryPolicy(wait=5.0, backoff=1.0)
    assert policy.delay(1) == 5.0
    assert policy.delay(2) == 5.0
    assert policy.delay(10) == 5.0


async def test_delay_exponential():
    policy = RetryPolicy(wait=1.0, backoff=2.0)
    assert policy.delay(1) == 1.0
    assert policy.delay(2) == 2.0
    assert policy.delay(3) == 4.0


async def test_delay_capped_by_max_wait():
    policy = RetryPolicy(wait=10.0, backoff=2.0, max_wait=15.0)
    assert policy.delay(1) == 10.0
    assert policy.delay(2) == 15.0


async def test_matches_all_exceptions_when_empty():
    policy = RetryPolicy()
    assert policy.matches(RuntimeError("any"))
    assert policy.matches(ValueError("also"))


async def test_matches_specific_exception_type():
    policy = RetryPolicy(exceptions=(ValueError,))
    assert policy.matches(ValueError("yes"))
    assert not policy.matches(RuntimeError("no"))


async def test_matches_subclass():
    policy = RetryPolicy(exceptions=(Exception,))
    assert policy.matches(RuntimeError("subclass of Exception"))


# ---------------------------------------------------------------------------
# Helpers for TaskRun unit tests
# ---------------------------------------------------------------------------


def _make_task_run(t, task_manager: TaskScheduler, **kwargs) -> TaskRun:
    return TaskRun(
        id=create_uid(),
        task=t,
        priority=TaskPriority.medium,
        params=t.params_model(),
        task_manager=task_manager,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# _maybe_failure_retry unit tests
# ---------------------------------------------------------------------------


@task(retry=RetryPolicy(max_attempts=2, wait=0.5, backoff=2.0))
async def _retry_task(ctx: TaskRun) -> None:
    pass


@task
async def _no_retry_task(ctx: TaskRun) -> None:
    pass


@task(retry=RetryPolicy(max_attempts=1, exceptions=(ValueError,)))
async def _typed_retry_task(ctx: TaskRun) -> None:
    pass


@task(retry=RetryPolicy(max_attempts=None, wait=0.0))
async def _unlimited_retry_task(ctx: TaskRun) -> None:
    pass


async def test_failure_retry_no_policy_returns_none(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_no_retry_task, task_scheduler)
    assert task_run._maybe_failure_retry(RuntimeError("boom")) is None


async def test_failure_retry_returns_copy_on_first_attempt(
    task_scheduler: TaskScheduler,
):
    task_run = _make_task_run(_retry_task, task_scheduler)
    retry = task_run._maybe_failure_retry(RuntimeError("boom"))
    assert retry is not None
    assert retry.retry_attempt == 1
    assert retry.execute_after is not None
    assert retry.state == TaskState.init
    assert retry.queued is None
    assert retry.start is None
    assert retry.end is None


async def test_failure_retry_increments_attempt(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_retry_task, task_scheduler, retry_attempt=1)
    retry = task_run._maybe_failure_retry(RuntimeError("boom"))
    assert retry is not None
    assert retry.retry_attempt == 2


async def test_failure_retry_exhausted_returns_none(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_retry_task, task_scheduler, retry_attempt=2)
    assert task_run._maybe_failure_retry(RuntimeError("boom")) is None


async def test_failure_retry_unmatched_exception_returns_none(
    task_scheduler: TaskScheduler,
):
    task_run = _make_task_run(_typed_retry_task, task_scheduler)
    assert task_run._maybe_failure_retry(RuntimeError("wrong type")) is None


async def test_failure_retry_matched_exception_returns_copy(
    task_scheduler: TaskScheduler,
):
    task_run = _make_task_run(_typed_retry_task, task_scheduler)
    retry = task_run._maybe_failure_retry(ValueError("right type"))
    assert retry is not None
    assert retry.retry_attempt == 1


async def test_failure_retry_unlimited_attempts(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_unlimited_retry_task, task_scheduler, retry_attempt=999)
    retry = task_run._maybe_failure_retry(RuntimeError("always retry"))
    assert retry is not None
    assert retry.retry_attempt == 1000


# ---------------------------------------------------------------------------
# _maybe_rate_limit_retry unit tests
# ---------------------------------------------------------------------------


@task(max_concurrency=1, rate_limit_retry=RetryPolicy(max_attempts=2, wait=0.5))
async def _rate_limit_retry_task(ctx: TaskRun) -> None:
    pass


@task(max_concurrency=1)
async def _rate_limit_no_retry_task(ctx: TaskRun) -> None:
    pass


async def test_rate_limit_retry_not_triggered_when_under_limit(
    task_scheduler: TaskScheduler,
):
    task_run = _make_task_run(_rate_limit_retry_task, task_scheduler)
    result = task_run._maybe_rate_limit_retry(current_runs=0)
    assert result is None
    assert task_run.state == TaskState.init


async def test_rate_limit_retry_returns_copy_when_at_limit(
    task_scheduler: TaskScheduler,
):
    task_run = _make_task_run(_rate_limit_retry_task, task_scheduler)
    retry = task_run._maybe_rate_limit_retry(current_runs=1)
    assert retry is not None
    assert retry.rate_limit_attempt == 1
    assert retry.execute_after is not None


async def test_rate_limit_retry_exhausted_sets_rate_limited_state(
    task_scheduler: TaskScheduler,
):
    task_run = _make_task_run(
        _rate_limit_retry_task, task_scheduler, rate_limit_attempt=2
    )
    result = task_run._maybe_rate_limit_retry(current_runs=1)
    assert result is None
    assert task_run.state == TaskState.rate_limited


async def test_rate_limit_no_retry_policy_sets_rate_limited_state(
    task_scheduler: TaskScheduler,
):
    task_run = _make_task_run(_rate_limit_no_retry_task, task_scheduler)
    result = task_run._maybe_rate_limit_retry(current_runs=1)
    assert result is None
    assert task_run.state == TaskState.rate_limited


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


async def test_retry_succeeds_after_failure(task_scheduler: TaskScheduler) -> None:
    initial_info = await task_scheduler.broker.get_tasks_info("retryable")
    initial_end = initial_info[0].last_run_end

    await task_scheduler.queue("retryable", fail_times=1)

    async def retry_succeeded() -> bool:
        info = await task_scheduler.broker.get_tasks_info("retryable")
        ti = info[0]
        return ti.last_run_end != initial_end and ti.last_run_state == TaskState.success

    await wait_for(retry_succeeded, timeout=10)


async def test_retry_exhausted_ends_in_failure(task_scheduler: TaskScheduler) -> None:
    initial_info = await task_scheduler.broker.get_tasks_info("retryable")
    initial_end = initial_info[0].last_run_end

    await task_scheduler.queue("retryable", fail_times=4)

    async def retry_failed() -> bool:
        info = await task_scheduler.broker.get_tasks_info("retryable")
        ti = info[0]
        return ti.last_run_end != initial_end and ti.last_run_state == TaskState.failure

    await wait_for(retry_failed, timeout=10)


# ---------------------------------------------------------------------------
# TaskRun property tests
# ---------------------------------------------------------------------------


async def test_task_run_name_id(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_retry_task, task_scheduler)
    assert task_run.name_id == f"{_retry_task.name}.{task_run.id}"


async def test_task_run_in_queue(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_retry_task, task_scheduler)
    assert task_run.in_queue is None
    task_run.set_state(TaskState.running)
    assert task_run.in_queue is not None


async def test_task_run_duration_and_total(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_retry_task, task_scheduler)
    assert task_run.duration is None
    assert task_run.duration_ms is None
    assert task_run.total is None
    task_run.set_state(TaskState.success)
    assert task_run.duration is not None
    assert task_run.duration_ms is not None
    assert task_run.total is not None


async def test_set_state_same_state_is_noop(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_retry_task, task_scheduler)
    task_run.set_state(TaskState.init)
    assert task_run.state == TaskState.init


async def test_set_state_invalid_transition_raises(task_scheduler: TaskScheduler):
    task_run = _make_task_run(_retry_task, task_scheduler)
    task_run.set_state(TaskState.success)
    with pytest.raises(TaskRunError):
        task_run.set_state(TaskState.running)


# ---------------------------------------------------------------------------
# task() decorator error cases
# ---------------------------------------------------------------------------


async def test_task_decorator_error_positional_and_kwargs():
    async def my_func(ctx: TaskRun) -> None:
        pass

    with pytest.raises(TaskDecoratorError):
        task(my_func, name="foo")  # type: ignore[call-overload]


async def test_task_decorator_error_no_executor_no_kwargs():
    with pytest.raises(TaskDecoratorError):
        task()


# ---------------------------------------------------------------------------
# rate_limit_retry integration test (covers consumer._register_task_run 517-518)
# ---------------------------------------------------------------------------


async def test_rate_limit_retry_requeues_and_succeeds(
    task_scheduler: TaskScheduler,
) -> None:
    initial_info = await task_scheduler.broker.get_tasks_info("exclusive")
    initial_end = initial_info[0].last_run_end

    # queue two concurrent runs — the second will be rate-limited and retried
    await task_scheduler.queue("exclusive", sleep=0.5)
    await task_scheduler.queue("exclusive", sleep=0.1)

    async def both_done() -> bool:
        info = await task_scheduler.broker.get_tasks_info("exclusive")
        ti = info[0]
        return ti.last_run_end != initial_end and ti.last_run_state == TaskState.success

    await wait_for(both_done, timeout=10)


# ---------------------------------------------------------------------------
# _re_queue_if_not_ready unit tests
# ---------------------------------------------------------------------------


async def test_re_queue_no_execute_after(task_scheduler: TaskScheduler) -> None:
    task_run = _make_task_run(_retry_task, task_scheduler)
    assert task_run.execute_after is None
    assert task_scheduler._re_queue_if_not_ready(task_run) is False


async def test_re_queue_execute_after_in_past(task_scheduler: TaskScheduler) -> None:
    task_run = _make_task_run(
        _retry_task,
        task_scheduler,
        execute_after=utcnow() - timedelta(seconds=10),
    )
    assert task_scheduler._re_queue_if_not_ready(task_run) is False


async def test_re_queue_execute_after_in_future(task_scheduler: TaskScheduler) -> None:
    task_run = _make_task_run(
        _retry_task,
        task_scheduler,
        execute_after=utcnow() + timedelta(seconds=60),
    )
    before = monotonic()
    result = task_scheduler._re_queue_if_not_ready(task_run)
    assert result is True
    entries = [e for e in task_scheduler._in_process_queue._heap if e[2] is task_run]
    assert len(entries) == 1
    assert entries[0][0] >= before + 5


async def test_re_queue_minimum_delay_is_5_seconds(
    task_scheduler: TaskScheduler,
) -> None:
    # execute_after only 1 s away — delay must be clamped to the 5 s minimum
    task_run = _make_task_run(
        _retry_task,
        task_scheduler,
        execute_after=utcnow() + timedelta(seconds=1),
    )
    before = monotonic()
    task_scheduler._re_queue_if_not_ready(task_run)
    entries = [e for e in task_scheduler._in_process_queue._heap if e[2] is task_run]
    assert len(entries) == 1
    assert entries[0][0] >= before + 5
    assert entries[0][0] < before + 6


async def test_in_process_queue_drains_on_stop(
    task_scheduler: TaskScheduler,
) -> None:
    task_run1 = _make_task_run(_retry_task, task_scheduler)
    task_run2 = _make_task_run(_retry_task, task_scheduler)

    queue = InProcessTaskQueue(task_scheduler)
    queue.queue(task_run1, delay=100)
    queue.queue(task_run2, delay=100)
    assert queue.queue_len() == 2

    # worker never started so is_running() is False — drain runs immediately
    await queue.run()

    assert queue.queue_len() == 0
