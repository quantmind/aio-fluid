import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, NamedTuple, Optional, Union
from uuid import uuid4

from fluid import log
from fluid.utils import microseconds

from .constants import TaskPriority
from .crontab import ScheduleType
from .task_run import TaskRun

LogType = Callable[[str], None]

if TYPE_CHECKING:  # pragma: no cover
    from .broker import Broker
    from .consumer import TaskManager


class TaskDecoratorError(RuntimeError):
    pass


class TaskRunError(RuntimeError):
    pass


class TaskContext(NamedTuple):
    task_manager: "TaskManager"
    task_run: TaskRun
    log: LogType
    logger: logging.Logger

    @property
    def task(self) -> "Task":
        return self.task_run.task

    @property
    def run_id(self) -> "str":
        return self.task_run.id

    @property
    def name(self) -> str:
        return self.task_run.name

    @property
    def params(self) -> Dict[str, Any]:
        return self.task_run.params

    def raise_error(self, msg: str) -> None:
        raise TaskRunError(msg)


TaskExecutor = Callable[[TaskContext], None]
RandomizeType = Callable[[], Union[float, int]]


class Task(NamedTuple):
    """A Task execute any time it is invoked"""

    name: str
    executor: TaskExecutor
    logger: logging.Logger
    schedule: Optional[ScheduleType] = None
    randomize: Optional[RandomizeType] = None
    overlap: bool = True
    priority: TaskPriority = TaskPriority.medium

    @property
    def description(self) -> str:
        return self.executor.__doc__ or ""

    async def register(self, broker: "Broker") -> None:
        pass

    async def __call__(self, task_manager: "TaskManager", **kwargs) -> Any:
        context = self.create_context(task_manager, **kwargs)
        return await self.executor(context)

    def create_context(
        self,
        task_manager,
        task_run: Optional["TaskRun"] = None,
        log: Optional[LogType] = None,
        **params,
    ):
        if task_run is None:
            task_run = TaskRun(
                id=uuid4().hex, queued=microseconds(), task=self, params=params
            )
        else:
            assert task_run.name == self.name, "task_run for a different task"
            assert not params, "task run is provided - cannot pass params"
        return TaskContext(
            task_manager=task_manager,
            task_run=task_run,
            log=log or self.logger.info,
            logger=self.logger.getChild(task_run.id),
        )


def task(*args, **kwargs) -> Union[Task, "TaskConstructor"]:
    if kwargs and args:
        raise TaskDecoratorError("cannot use positional parameters")
    elif kwargs:
        return TaskConstructor(**kwargs)
    elif len(args) > 1:
        raise TaskDecoratorError("cannot use positional parameters")
    elif not args:
        raise TaskDecoratorError("this is a decorator cannot be invoked in this way")
    else:
        return TaskConstructor()(args[0])


class TaskConstructor:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, executor: TaskExecutor) -> Task:
        kwargs = {"name": executor.__name__, **self.kwargs, "executor": executor}
        name = kwargs["name"]
        kwargs["logger"] = log.get_logger(f"task.{name}")
        return Task(**kwargs)
