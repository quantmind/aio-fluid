from __future__ import annotations

import asyncio
import enum
import inspect
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generic,
    NamedTuple,
    TypeVar,
    overload,
)

from pydantic import BaseModel, Field, field_serializer
from redis.asyncio.lock import Lock
from typing_extensions import Annotated, Doc

from fluid import settings
from fluid.utils import kernel, log
from fluid.utils.data import compact_dict
from fluid.utils.dates import as_utc
from fluid.utils.text import create_uid, trim_docstring

from .common import cpu_env, is_in_cpu_process
from .errors import TaskDecoratorError, TaskRunError
from .scheduler_crontab import Scheduler

try:
    from .k8s_job import run_on_k8s_job
except ImportError:  # pragma: no cover
    run_on_k8s_job = None  # type: ignore[assignment]


if TYPE_CHECKING:
    from .consumer import TaskManager


TaskExecutor = Callable[["TaskRun"], Coroutine[Any, Any, Any]]
RandomizeType = Callable[[], float | int]
TP = TypeVar("TP", bound=BaseModel)


class EmptyParams(BaseModel):
    pass


class TaskPriority(enum.StrEnum):
    high = enum.auto()
    medium = enum.auto()
    low = enum.auto()


class TaskState(enum.StrEnum):
    init = enum.auto()
    queued = enum.auto()
    running = enum.auto()
    success = enum.auto()
    failure = enum.auto()
    aborted = enum.auto()
    rate_limited = enum.auto()

    @property
    def is_failure(self) -> bool:
        return self is TaskState.failure

    @property
    def is_done(self) -> bool:
        return self in FINISHED_STATES


FINISHED_STATES = frozenset(
    (TaskState.success, TaskState.failure, TaskState.aborted, TaskState.rate_limited)
)


class TaskManagerConfig(BaseModel):
    """Task manager configuration"""

    schedule_tasks: bool = Field(default=True, description="Schedule tasks or sleep")
    consume_tasks: bool = Field(default=True, description="Consume tasks or sleep")
    max_concurrent_tasks: int = Field(
        default=settings.MAX_CONCURRENT_TASKS,
        description=(
            "The number of coroutine workers consuming tasks. "
            "Each worker consumes one task at a time, therefore, "
            "this number is the maximum number of tasks that can run concurrently."
            "It can be configured via the `FLUID_MAX_CONCURRENT_TASKS` environment "
            "variable, and by default is set to 5."
        ),
    )
    sleep_millis: int = Field(
        default=settings.SLEEP_MILLIS,
        description=(
            "Milliseconds to async sleep when no tasks available to consume."
            "This value can be configured via the `FLUID_SLEEP_MILLIS` environment "
            "variable, and by default is set to 1000 milliseconds (1 second)."
        ),
    )
    broker_url: str = ""

    @property
    def sleep(self) -> float:
        """Sleep time in seconds"""
        return self.sleep_millis / 1000.0


class TaskInfoBase(BaseModel):
    name: str = Field(description="Task name")
    description: str = Field(description="Task description")
    module: str = Field(description="Task module")
    priority: TaskPriority = Field(description="Task priority")
    schedule: str | None = Field(default=None, description="Task schedule")


class TaskInfoUpdate(BaseModel):
    enabled: bool = Field(default=True, description="Task enabled")
    last_run_end: datetime | None = Field(
        default=None, description="Task last run end as milliseconds since epoch"
    )
    last_run_duration: timedelta | None = Field(
        default=None, description="Task last run duration in milliseconds"
    )
    last_run_state: TaskState | None = Field(
        default=None, description="State of last task run"
    )


class TaskInfo(TaskInfoBase, TaskInfoUpdate):
    pass


class QueuedTask(BaseModel):
    """A task to be queued"""

    run_id: str = Field(description="Task run id")
    task: str = Field(description="Task name")
    params: dict[str, Any] = Field(description="Task parameters")
    priority: TaskPriority | None = Field(default=None, description="Task priority")


class K8sConfig(NamedTuple):
    """Kubernetes configuration"""

    namespace: str = "default"
    """Kubernetes namespace where the task consumer deployment run"""
    deployment: str = "fluid-task"
    """Kubernetes deployment of the task consumer"""
    container: str = "main"
    """Kubernetes container"""
    job_ttl: int = 300
    """Time to live for k8s Job after completion"""
    sleep: float = 2.0
    """Amount to async sleep while waiting for completion of k8s Job"""

    @classmethod
    def from_env(cls) -> K8sConfig:
        return cls(
            namespace=os.getenv("FLUID_TASK_CONSUMER_K8S_NAMESPACE", "default"),
            deployment=os.getenv("FLUID_TASK_CONSUMER_K8S_DEPLOYMENT", "fluid-task"),
            container=os.getenv("FLUID_TASK_CONSUMER_K8S_CONTAINER", "main"),
            job_ttl=int(os.getenv("FLUID_TASK_CONSUMER_K8S_JOB_TTL", "300")),
        )


class Task(NamedTuple, Generic[TP]):
    """A Task executes any time it is invoked"""

    name: str
    """Task name - unique identifier"""
    executor: TaskExecutor
    """Task executor function"""
    params_model: Annotated[type[TP], Doc("Pydantic model for task parameters")]
    logger: logging.Logger
    """Task logger"""
    module: str = ""
    """Task python module"""
    short_description: str = ""
    """Short task description - one line"""
    description: str = ""
    """Task description - obtained from the executor docstring if not provided"""
    schedule: Scheduler | None = None
    """Task schedule - None means the task is not scheduled"""
    randomize: RandomizeType | None = None
    """Randomize function for task schedule"""
    max_concurrency: int = 0
    """how many tasks can be run concurrently - 0 means no limit"""
    timeout_seconds: int = 60
    """Task timeout in seconds - how long the task can run before being aborted"""
    priority: TaskPriority = TaskPriority.medium
    """Task priority - high, medium, low"""
    k8s_config: K8sConfig = K8sConfig.from_env()

    @property
    def cpu_bound(self) -> bool:
        """True if the task is CPU bound"""
        return self.executor is run_in_subprocess

    def info(self, **params: Any) -> TaskInfo:
        """Return task info object"""
        params.update(
            name=self.name,
            description=self.description,
            module=self.module,
            priority=self.priority,
            schedule=str(self.schedule) if self.schedule else None,
        )
        return TaskInfo(**compact_dict(params))


class TaskRun(BaseModel, Generic[TP], arbitrary_types_allowed=True):
    """A TaskRun contains all the data generated by a Task run

    This model is never initialized directly, it is created by the TaskManager
    """

    id: str = Field(description="Unique task run id")
    task: Task = Field(description="Task to be executed")
    priority: TaskPriority = Field(description="Task priority")
    params: TP = Field(description="Task parameters")
    state: TaskState = Field(default=TaskState.init, description="Task state")
    task_manager: TaskManager = Field(exclude=True, repr=False)
    queued: datetime | None = None
    start: datetime | None = None
    end: datetime | None = None

    async def execute(self) -> None:
        """Execute the task"""
        try:
            self.set_state(TaskState.running)
            async with asyncio.timeout(self.task.timeout_seconds):
                await self.task.executor(self)  # type: ignore [arg-type]
        except Exception:
            self.set_state(TaskState.failure)
            raise
        else:
            self.set_state(TaskState.success)

    @field_serializer("task")
    def _serialize_task(self, task: Task, _info: Any) -> str:
        return task.name

    @property
    def logger(self) -> logging.Logger:
        return self.task.logger

    @property
    def in_queue(self) -> timedelta | None:
        if self.queued and self.start:
            return self.start - self.queued
        return None

    @property
    def duration(self) -> timedelta | None:
        if self.start and self.end:
            return self.end - self.start
        return None

    @property
    def duration_ms(self) -> float | None:
        duration = self.duration
        if duration is not None:
            return round(1000 * duration.total_seconds(), 2)
        return None

    @property
    def total(self) -> timedelta | None:
        if self.queued and self.end:
            return self.end - self.queued
        return None

    @property
    def name(self) -> str:
        return self.task.name

    @property
    def name_id(self) -> str:
        return f"{self.task.name}.{self.id}"

    @property
    def is_done(self) -> bool:
        return self.state.is_done

    @property
    def is_failure(self) -> bool:
        return self.state.is_failure

    @property
    def deps(self) -> Any:
        return self.task_manager.deps

    def set_state(
        self,
        state: TaskState,
        state_time: datetime | None = None,
    ) -> None:
        if self.state == state:
            return
        state_time = as_utc(state_time)
        match (self.state, state):
            case (TaskState.init, TaskState.queued):
                self.queued = state_time
                self.state = state
                self._dispatch()
            case (TaskState.init, _):
                self.set_state(TaskState.queued, state_time)
                self.set_state(state, state_time)
            case (TaskState.queued, TaskState.running):
                self.start = state_time
                self.state = state
                self._dispatch()
            case (
                TaskState.queued,
                TaskState.success
                | TaskState.aborted
                | TaskState.rate_limited
                | TaskState.failure,
            ):
                self.set_state(TaskState.running, state_time)
                self.set_state(state, state_time)
            case (
                TaskState.running,
                TaskState.success
                | TaskState.aborted
                | TaskState.rate_limited
                | TaskState.failure,
            ):
                self.end = state_time
                self.state = state
                self._dispatch()
            case _:
                raise TaskRunError(f"invalid state transition {self.state} -> {state}")

    def lock(self, timeout: float | None) -> Lock:
        return self.task_manager.broker.lock(self.name, timeout=timeout)

    def _dispatch(self) -> None:
        self.task_manager.dispatcher.dispatch(self.model_copy())  # type: ignore [arg-type]


@dataclass
class TaskRunWaiter:
    task_manager: TaskManager
    uid: str = field(default_factory=create_uid)
    _runs: dict[str, TaskRun] = field(default_factory=dict)

    def event(self, state: TaskState) -> str:
        return f"{state}.{self.uid}"

    def __enter__(self) -> TaskRunWaiter:
        for state in FINISHED_STATES:
            self.task_manager.dispatcher.register_handler(self.event(state), self)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        for state in FINISHED_STATES:
            self.task_manager.dispatcher.unregister_handler(self.event(state))

    def __call__(self, task_run: TaskRun) -> None:
        self._runs[task_run.id] = task_run

    async def wait(self, task_run: TaskRun, *, timeout: int | None = None) -> TaskRun:
        timeout = timeout or task_run.task.timeout_seconds
        async with asyncio.timeout(timeout):
            while True:
                if tr := self._runs.get(task_run.id):
                    if tr.is_done:
                        return tr
                await asyncio.sleep(0.01)


@overload
def task(executor: TaskExecutor) -> Task: ...


@overload
def task(
    *,
    name: str | None = None,
    schedule: Scheduler | None = None,
    short_description: str | None = None,
    description: str | None = None,
    randomize: RandomizeType | None = None,
    max_concurrency: int = 0,
    priority: TaskPriority = TaskPriority.medium,
    cpu_bound: bool = False,
    k8s_config: Annotated[
        K8sConfig | None,
        Doc("Kubernetes configuration - None means use the default configuration"),
    ] = None,
    timeout_seconds: Annotated[
        int,
        Doc("Task timeout in seconds - how long the task can run before being aborted"),
    ] = 60,
) -> TaskConstructor: ...


# implementation of the task decorator
def task(executor: TaskExecutor | None = None, **kwargs: Any) -> Task | TaskConstructor:
    """Decorator to create a Task

    This decorator can be used in two ways:

    - As a simple decorator of the executor function
    - As a function with keyword arguments
    """
    if kwargs and executor:
        raise TaskDecoratorError("cannot use positional parameters")
    elif kwargs:
        return TaskConstructor(**kwargs)
    elif not executor:
        raise TaskDecoratorError("this is a decorator cannot be invoked in this way")
    else:
        return TaskConstructor()(executor)


class TaskConstructor:
    def __init__(self, *, cpu_bound: bool = False, **kwargs: Any) -> None:
        self.cpu_bound = cpu_bound
        self.kwargs = kwargs

    def __call__(self, executor: TaskExecutor) -> Task:
        if self.cpu_bound:
            return self.cpu_bound_task(executor)
        else:
            return self.create_task(executor)

    def create_task(
        self,
        executor: TaskExecutor,
        defaults: dict[str, Any] | None = None,
    ) -> Task:
        kwargs: dict[str, Any] = self.kwargs_defaults(executor)
        if defaults:
            kwargs.update(defaults)
        kwargs.update(compact_dict(self.kwargs))
        name = kwargs["name"]
        kwargs.update(
            executor=executor,
            params_model=self.get_params_model(executor),
            logger=log.get_logger(f"task.{name}", prefix=True),
        )
        return Task(**kwargs)

    def cpu_bound_task(self, executor: TaskExecutor) -> Task:
        if is_in_cpu_process():
            return self.create_task(executor)
        else:
            return self.create_task(run_cpu_bound, self.kwargs_defaults(executor))

    def kwargs_defaults(self, executor: TaskExecutor) -> dict[str, Any]:
        module = inspect.getmodule(executor)
        description = trim_docstring(inspect.getdoc(executor) or "")
        short_description = description.split("\n")[0].strip()
        return dict(
            name=get_name(executor),
            module=module.__name__ if module else "",
            description=description,
            short_description=short_description,
            executor=executor,
        )

    def get_params_model(self, executor: TaskExecutor) -> type[BaseModel]:
        signature = inspect.signature(executor)
        for p in signature.parameters.values():
            if is_subclass(p.annotation, TaskRun):
                params_model = p.annotation.model_fields["params"]
                a = params_model.annotation
                return a if is_subclass(a, BaseModel) else EmptyParams
        return EmptyParams


def get_name(o: Any) -> str:
    if hasattr(o, "__name__"):
        return str(o.__name__)
    elif hasattr(o, "__class__"):
        return str(o.__class__.__name__)
    else:
        return str(o)


def is_subclass(cls: Any, parent: type) -> bool:
    try:
        return issubclass(cls, parent)
    except TypeError:
        return False


class RemoteLog:
    def __init__(self, out: Any) -> None:
        self.out = out

    def __call__(self, data: bytes) -> None:
        self.out.write(data.decode("utf-8"))


async def run_in_subprocess(ctx: TaskRun[TP]) -> None:
    env = dict(os.environ)
    env.update(cpu_env())
    result = await kernel.run_python(
        "-W",
        "ignore",
        "-m",
        "fluid.scheduler.cpubound",
        ctx.name,
        ctx.task.module,
        ctx.id,
        ctx.params.model_dump_json(),
        result_callback=RemoteLog(sys.stdout),
        error_callback=RemoteLog(sys.stderr),
        env=env,
        stream_output=True,
        stream_error=True,
    )
    if result:
        raise TaskRunError(result)


run_cpu_bound = run_in_subprocess
if os.getenv("KUBERNETES_SERVICE_HOST") and run_on_k8s_job is not None:
    run_cpu_bound = run_on_k8s_job
