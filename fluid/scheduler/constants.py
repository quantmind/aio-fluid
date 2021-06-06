import enum


class TaskPriority(enum.Enum):
    high = 1
    medium = 2
    low = 3


class TaskState(enum.Enum):
    init = 0
    queued = 1
    running = 2
    success = 3
    failure = 4


FINISHED_STATES = frozenset((TaskState.success, TaskState.failure))
