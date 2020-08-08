import logging
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Dict, NamedTuple, Optional

LogType = Callable[[str], None]

if TYPE_CHECKING:  # pragma: no cover
    from .broker import Broker


class TaskRunError(RuntimeError):
    pass


class TaskContext(NamedTuple):
    app: Any
    task: "Task"
    log: LogType
    params: Dict[str, Any]

    def raise_error(self, msg: str) -> None:
        raise TaskRunError(msg)


TaskExecutor = Callable[[TaskContext], None]


@dataclass
class Task:
    """A Task execute any time it is invoked"""

    name: str
    executor: TaskExecutor

    @cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"task.{self.name}")

    async def register(self, broker: "Broker") -> None:
        pass

    async def __call__(self, app: Any, **kwargs) -> Any:
        context = self.create_context(app, **kwargs)
        return await self.executor(context)

    def create_context(self, app, log: Optional[LogType] = None, **params):
        return TaskContext(
            app=app, task=self, log=log or self.logger.info, params=params
        )


def task(executor) -> Task:
    return Task(name=executor.__name__, executor=executor)
