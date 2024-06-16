from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from contextlib import AsyncExitStack
from functools import cached_property, partial
from typing import Any, Callable, Coroutine, NamedTuple, Self

from inflection import underscore

from fluid.utils import log
from fluid.utils.dates import utcnow
from fluid.utils.worker import WorkerFunction, Workers

from .broker import Broker, TaskRegistry
from .errors import TaskRunError, UnknownTaskError
from .models import Task, TaskManagerConfig, TaskPriority, TaskRun, TaskState

try:
    from .cli import TaskManagerCLI
except ImportError:
    TaskManagerCLI = None  # type: ignore[assignment,misc]


ConsumerCallback = Callable[[TaskRun, "TaskManager"], None]
AsyncExecutor = Callable[..., Coroutine[Any, Any, None]]
AsyncMessage = tuple[AsyncExecutor, tuple[Any, ...]]

logger = log.get_logger(__name__)


class Event(NamedTuple):
    type: str
    tag: str

    @classmethod
    def from_string(cls, event: str) -> "Event":
        bits = event.split(".")
        return cls(bits[0], bits[1] if len(bits) > 1 else "")


class TaskManager:
    """The task manager is the main entry point for managing tasks"""

    def __init__(self, **kwargs: Any) -> None:
        self.state: dict[str, Any] = {}
        self.config: TaskManagerConfig = TaskManagerConfig(**kwargs)
        self._msg_handlers: dict[str, dict[str, ConsumerCallback]] = defaultdict(dict)
        self._stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        try:
            await self._stack.aclose()
        finally:
            await self.on_shutdown()

    async def enter_async_context(self, cm: Any) -> Any:
        return await self._stack.enter_async_context(cm)

    @cached_property
    def broker(self) -> Broker:
        return Broker.from_url(self.config.broker_url)

    @property
    def registry(self) -> TaskRegistry:
        return self.broker.registry

    @property
    def type(self) -> str:
        return underscore(self.__class__.__name__)

    async def execute(self, task: Task | str, **params: Any) -> TaskRun:
        """Execute a task and wait for it to finish"""
        task_run = self.create_task_run(task, **params)
        await task_run.execute()
        return task_run

    async def on_shutdown(self) -> None:
        await self.broker.close()

    def execute_sync(self, task: Task | str, **params: Any) -> TaskRun:
        return asyncio.get_event_loop().run_until_complete(
            self._execute_and_exit(task, **params)
        )

    def register_task(self, task: Task) -> None:
        """Register a task with the task manager

        Only tasks registered can be executed by a task manager
        """
        self.broker.register_task(task)

    async def queue(
        self,
        task: str | Task,
        priority: TaskPriority | None = None,
        **params: Any,
    ) -> TaskRun:
        task_run = self.create_task_run(task, priority=priority, **params)
        await self.broker.queue_task(task_run)
        self.dispatch(task_run, "queued")
        return task_run

    def create_task_run(
        self,
        task: str | Task,
        run_id: str = "",
        priority: TaskPriority | None = None,
        **params: Any,
    ) -> TaskRun:
        """Create a TaskRun"""
        if isinstance(task, str):
            task = self.broker.task_from_registry(task)
        run_id = run_id or self.broker.new_uuid()
        return TaskRun(
            id=run_id,
            task=task,
            priority=priority or task.priority,
            state=TaskState.queued,
            params=params,
            queued=utcnow(),
            task_manager=self,
        )

    def dispatch(self, task_run: TaskRun, event_type: str) -> None:
        """Dispatch a message to the registered handlers."""
        if handlers := self._msg_handlers.get(event_type):
            for handler in handlers.values():
                handler(task_run, self)

    def register_handler(self, event_name: str, handler: ConsumerCallback) -> None:
        event = Event.from_string(event_name)
        self._msg_handlers[event.type][event.tag] = handler

    def unregister_handler(self, event_name: str) -> None:
        event = Event.from_string(event_name)
        self._msg_handlers[event.type].pop(event.tag, None)

    def register_from_module(self, module: Any) -> None:
        for name in dir(module):
            if name.startswith("_"):
                continue
            if isinstance(obj := getattr(module, name), Task):
                self.register_task(obj)

    def cli(self, **kwargs: Any) -> Any:
        """Create the task manager command line interface"""
        try:
            from fluid.scheduler.cli import TaskManagerCLI
        except ImportError:
            raise ImportError(
                "TaskManagerCLI is not available - "
                "install with `pip install aio-fluid[cli]`"
            ) from None
        return TaskManagerCLI(self, **kwargs)

    async def _execute_and_exit(self, task: Task | str, **params: Any) -> TaskRun:
        async with self:
            return await self.execute(task, **params)


class TaskConsumer(TaskManager, Workers):
    """The Task Consumer is a Task Manager responsible for consuming tasks
    from a task queue
    """

    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
        Workers.__init__(self)
        self._concurrent_tasks: dict[str, dict[str, TaskRun]] = defaultdict(dict)
        self._task_to_queue: deque[str | Task] = deque()
        self._priority_task_run_queue: deque[TaskRun] = deque()
        self._queue_tasks_worker = WorkerFunction(
            self._queue_task, name="queue-task-worker"
        )
        for i in range(self.config.max_concurrent_tasks):
            worker_name = f"task-worker-{i+1}"
            self.add_workers(
                WorkerFunction(
                    partial(self._consume_tasks, worker_name), name=worker_name
                )
            )

    @property
    def num_concurrent_tasks(self) -> int:
        """The number of concurrent_tasks"""
        return sum(len(v) for v in self._concurrent_tasks.values())

    def sync_queue(self, task: str | Task) -> None:
        self._task_to_queue.appendleft(task)

    def sync_priority_queue(self, task: str | Task) -> None:
        self._priority_task_run_queue.appendleft(self.create_task_run(task))

    def num_concurrent_tasks_for(self, task_name: str) -> int:
        """The number of concurrent tasks for a given task_name"""
        return len(self._concurrent_tasks[task_name])

    # Internals

    # process tasks from the internal queue
    async def _queue_task(self) -> None:
        try:
            task = self._task_to_queue.pop()
        except IndexError:
            await asyncio.sleep(0.1)
        else:
            await self.queue(task)
            await asyncio.sleep(0)

    async def _consume_tasks(self, worker_name: str) -> None:
        if not self.config.consume_tasks:
            await asyncio.sleep(self.config.sleep)
            return
        if self._priority_task_run_queue:
            task_run = self._priority_task_run_queue.pop()
        else:
            try:
                maybe_task_run = await self.broker.get_task_run(self)
            except UnknownTaskError as exc:
                logger.error(
                    "%s unknown task %s - it looks like it is not "
                    "registered with this consumer",
                    worker_name,
                    exc,
                )
                maybe_task_run = None
            if not maybe_task_run:
                return
            else:
                task_run = maybe_task_run
        task_name = task_run.name
        self._concurrent_tasks[task_name][task_run.id] = task_run
        #
        if (
            task_run.task.max_concurrency > 0
            and task_run.task.max_concurrency < self.num_concurrent_tasks_for(task_name)
        ):
            task_run.set_state(TaskState.rate_limited)
        elif not (await self.broker.get_tasks_info(task_name))[0].enabled:
            task_run.set_state(TaskState.aborted)
        #
        else:
            task_run.logger.info("start")
            self.dispatch(task_run, "start")
        try:
            await task_run.execute()
        except TaskRunError:
            # no logging as this was a controlled exception
            pass
        except Exception:
            task_run.logger.exception("critical exception while executing")
        self._concurrent_tasks[task_name].pop(task_run.id, None)
        await self.broker.update_task(
            task_run.task,
            dict(
                last_run_end=task_run.end,
                last_run_duration=task_run.duration,
                last_run_state=task_run.state,
            ),
        )
        duration = task_run.duration_ms
        if duration is not None:
            self.dispatch(task_run, "end")
            task_run.logger.log(
                logging.WARNING if task_run.is_failure else logging.INFO,
                "end - %s - milliseconds - %s",
                task_run.state,
                duration,
            )


# required by pydantic to avoid `Class not fully defined` error
TaskRun.model_rebuild()
