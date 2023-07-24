from __future__ import annotations

import inspect
import logging
import os
from importlib import import_module
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    NamedTuple,
    TypeVar,
    cast,
    overload,
)
from uuid import uuid4

from openapi.spec.utils import trim_docstring

from fluid import log
from fluid.node import WorkerApplication
from fluid.utils import microseconds

from .constants import TaskPriority
from .crontab import Scheduler
from .task_run import TaskRun

LogType = Callable[[str], None]

if TYPE_CHECKING:  # pragma: no cover
    from .broker import Broker
    from .consumer import TaskManager


class TaskDecoratorError(RuntimeError):
    pass


class TaskRunError(RuntimeError):
    pass


TASK_MANAGER_APP: str = os.getenv("TASK_MANAGER_APP", "")
T = TypeVar("T")


class ImproperlyConfigured(RuntimeError):
    pass


def create_task_app() -> WorkerApplication:
    if not TASK_MANAGER_APP:
        raise ImproperlyConfigured("missing TASK_MANAGER_APP environment variable")
    bits = TASK_MANAGER_APP.split(":")
    if len(bits) != 2:
        raise ImproperlyConfigured(
            "TASK_MANAGER_APP must be of the form <module>:<function>"
        )
    mod = import_module(bits[0])
    return cast(WorkerApplication, getattr(mod, bits[1])())


class TaskContext(NamedTuple):
    task_manager: "TaskManager"
    task_run: TaskRun
    log: LogType
    logger: logging.Logger

    @property
    def task(self) -> Task:
        return self.task_run.task

    @property
    def run_id(self) -> str:
        return self.task_run.id

    @property
    def name(self) -> str:
        return self.task_run.name

    @property
    def params(self) -> dict[str, Any]:
        return self.task_run.params

    def model(self, factory: type[T]) -> T:
        """Create a model instance from the task params"""
        return factory(**self.task_run.params)

    def raise_error(self, msg: str) -> None:
        raise TaskRunError(msg)


TaskExecutor = Callable[[TaskContext], Coroutine[Any, Any, Any]]
RandomizeType = Callable[[], float | int]


class Task(NamedTuple):
    """A Task execute any time it is invoked"""

    name: str
    executor: TaskExecutor
    logger: logging.Logger
    description: str = ""
    schedule: Scheduler | None = None
    randomize: RandomizeType | None = None
    max_concurrency: int = 1
    """how many tasks can run in each consumer concurrently"""
    priority: TaskPriority = TaskPriority.medium

    async def register(self, broker: "Broker") -> None:
        pass

    async def __call__(
        self,
        task_manager: TaskManager | None = None,
        **kwargs: Any,
    ) -> Any:
        if task_manager is None:
            task_manager = create_task_app()["task_manager"]
        context = self.create_context(task_manager, **kwargs)
        return await self.executor(context)

    def create_context(
        self,
        task_manager: TaskManager,
        task_run: TaskRun | None = None,
        log: LogType | None = None,
        **params: Any,
    ) -> TaskContext:
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


@overload
def task(executor: TaskExecutor) -> Task:
    ...


@overload
def task(
    *,
    name: str | None = None,
    schedule: Scheduler | None = None,
    description: str | None = None,
    randomize: RandomizeType | None = None,
    max_concurrency: int = 1,
    priority: TaskPriority = TaskPriority.medium,
) -> TaskConstructor:
    ...


# implementation of the task decorator
def task(executor: TaskExecutor | None = None, **kwargs: Any) -> Task | TaskConstructor:
    if kwargs and executor:
        raise TaskDecoratorError("cannot use positional parameters")
    elif kwargs:
        return TaskConstructor(**kwargs)
    elif not executor:
        raise TaskDecoratorError("this is a decorator cannot be invoked in this way")
    else:
        return TaskConstructor()(executor)


class TaskConstructor:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def __call__(self, executor: TaskExecutor) -> Task:
        kwargs: dict[str, Any] = {
            "name": get_name(executor),
            "description": trim_docstring(inspect.getdoc(executor) or ""),
            "executor": executor,
        }
        kwargs.update(self.kwargs)
        name = kwargs["name"]
        kwargs["logger"] = log.get_logger(f"task.{name}")
        return Task(**kwargs)


def get_name(o: Any) -> str:
    if hasattr(o, "__name__"):
        return str(o.__name__)
    elif hasattr(o, "__class__"):
        return str(o.__class__.__name__)
    else:
        return str(o)
