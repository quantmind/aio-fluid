class TaskError(RuntimeError):
    pass


class UnknownTaskError(TaskError):
    pass


class DisabledTaskError(TaskError):
    pass


class TaskRunError(TaskError):
    pass


class TaskAbortedError(TaskError):
    pass


class TaskDecoratorError(TaskError):
    pass
