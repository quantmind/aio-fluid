import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, NamedTuple, Optional, Union

LogType = Callable[[str], None]

if TYPE_CHECKING:  # pragma: no cover
    from .broker import Broker


class TaskDecoratorError(RuntimeError):
    pass


class TaskRunError(RuntimeError):
    pass


class TaskContext(NamedTuple):
    app: Any
    task: "Task"
    log: LogType
    params: Dict[str, Any]

    @property
    def logger(self) -> logging.Logger:
        return self.task.logger

    def raise_error(self, msg: str) -> None:
        raise TaskRunError(msg)


TaskExecutor = Callable[[TaskContext], None]
ScheduleType = Callable[[datetime], bool]


class Task(NamedTuple):
    """A Task execute any time it is invoked"""

    name: str
    executor: TaskExecutor
    logger: logging.Logger
    schedule: Optional[ScheduleType] = None
    overlap: bool = True

    @property
    def description(self) -> str:
        return self.executor.__doc__ or ""

    async def register(self, broker: "Broker") -> None:
        pass

    async def __call__(self, app: Any, **kwargs) -> Any:
        context = self.create_context(app, **kwargs)
        return await self.executor(context)

    def create_context(self, app, log: Optional[LogType] = None, **params):
        return TaskContext(
            app=app, task=self, log=log or self.logger.info, params=params
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
