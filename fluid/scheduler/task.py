import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, NamedTuple, Optional, Union

from .crontab import ScheduleType

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
    task: "Task"
    run_id: str
    log: LogType
    params: Dict[str, Any]
    logger: logging.Logger

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
        log: Optional[LogType] = None,
        run_id: str = "",
        **params,
    ):
        return TaskContext(
            task_manager=task_manager,
            task=self,
            run_id=run_id,
            log=log or self.logger.info,
            params=params,
            logger=self.logger.getChild(run_id) if run_id else self.logger,
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
        kwargs["logger"] = logging.getLogger(f"task.{name}")
        return Task(**kwargs)
