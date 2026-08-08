from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .models import TaskRun


class TaskError(RuntimeError):
    """Base class for all task scheduler errors."""


class UnknownTaskError(TaskError):
    """Raised when a task name is not registered in the task registry."""


class DisabledTaskError(TaskError):
    """Raised when attempting to queue or run a disabled task."""


class TaskParamsError(TaskError):
    """Raised when task run parameters fail validation when consumed from the broker.

    It carries the task run, created with unvalidated params, so the consumer
    can mark it as failed and log the error.
    """

    def __init__(self, task_run: TaskRun, message: str) -> None:
        super().__init__(message)
        self.task_run = task_run


class TaskRunError(TaskError):
    """Raised when a task run fails during execution.

    This is an internal error used to signal a failure during task execution,
    and is not intended to be raised by user code.
    """


class TaskAbortedError(TaskError):
    """Raised when a task run is aborted before completion.

    If a task needs to abort itself it can raise this error, which will be caught
    by the consumer and treated as a soft-failure and therefore logged as
    info and not trigger any retry policy if configured.
    """


class TaskDecoratorError(TaskError):
    """Raised when a task is incorrectly decorated or configured."""


class CpuBoundEntryPointError(TaskError):
    """Raised when a cpu bound task cannot be executed by the entry point.

    Cpu bound tasks run in a separate process, started by the `exec` command of
    the task manager command line client. An application which does not expose
    one cannot execute them.
    """
