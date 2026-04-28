class TaskError(RuntimeError):
    """Base class for all task scheduler errors."""


class UnknownTaskError(TaskError):
    """Raised when a task name is not registered in the task registry."""


class DisabledTaskError(TaskError):
    """Raised when attempting to queue or run a disabled task."""


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
