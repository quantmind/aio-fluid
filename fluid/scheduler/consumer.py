from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from functools import cached_property, partial
from typing import Any, Callable, Coroutine, NamedTuple

from inflection import underscore

from fluid import settings
from .errors import TaskRunError, UnknownTaskError
from fluid.utils.dates import utcnow
from fluid.utils.worker import (
    QueueConsumerWorker,
    WorkerFunction,
    Workers,
)

from .broker import Broker, QueuedTask, TaskRegistry
from .models import Task, TaskManagerConfig, TaskPriority, TaskRun, TaskState

ConsumerCallback = Callable[[TaskRun, "TaskManager"], None]
AsyncExecutor = Callable[..., Coroutine[Any, Any, None]]
AsyncMessage = tuple[AsyncExecutor, tuple[Any, ...]]

logger = settings.get_logger(__name__)


class Event(NamedTuple):
    type: str
    tag: str

    @classmethod
    def from_string(cls, event: str) -> "Event":
        bits = event.split(".")
        return cls(bits[0], bits[1] if len(bits) > 1 else "")


class TaskManager(Workers):
    """Base class for both TaskConsumer and TaskScheduler"""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__()
        self._msg_handlers: dict[str, dict[str, ConsumerCallback]] = defaultdict(dict)
        self._task_to_queue: deque[QueuedTask] = deque()
        self._queue_tasks_worker = WorkerFunction(
            self._queue_task, name="queue-task-worker"
        )
        self._consumer = QueueConsumerWorker(self._execute_async)
        self.config: TaskManagerConfig = TaskManagerConfig(**kwargs)
        self.state: dict = {}
        self.add_workers(self._queue_tasks_worker, self._consumer)

    @cached_property
    def broker(self) -> Broker:
        return Broker.from_url(self.config.broker_url)

    @property
    def registry(self) -> TaskRegistry:
        return self.broker.registry

    @property
    def type(self) -> str:
        return underscore(self.__class__.__name__)

    async def on_shutdown(self) -> None:
        await self.broker.close()

    def register_task(self, task: Task) -> None:
        """Register a task with the task manager

        Only tasks registered can be executed by a task manager
        """
        self.broker.register_task(task)

    def register_from_module(self, module: Any) -> None:
        for name in dir(module):
            if name.startswith("_"):
                continue
            if isinstance(obj := getattr(module, name), Task):
                self.register_task(obj)

    def queue(
        self,
        task: str,
        priority: TaskPriority | None = None,
        **params: Any,
    ) -> QueuedTask:
        """Queue a Task for execution and return he QueuedTask object"""
        # make sure the task is registered
        self.broker.task_from_registry(task)
        queued_task = QueuedTask(
            run_id=self.broker.new_uuid(),
            task=task,
            priority=priority,
            params=params,
        )
        self._task_to_queue.appendleft(queued_task)
        return queued_task

    def create_task_run(self, queued_task: QueuedTask) -> TaskRun:
        """Create a TaskRun from a QueuedTask"""
        task = self.task_from_registry(queued_task.task)
        return self.create_task_run_from_task(
            task, queued_task.run_id, queued_task.priority, **queued_task.params
        )

    def create_task_run_from_task(
        self,
        task: Task,
        run_id: str,
        priority: TaskPriority | None = None,
        **params: Any,
    ) -> TaskRun:
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

    def execute_async(self, async_callable: AsyncExecutor, *args: Any) -> None:
        self._consumer.send((async_callable, args))

    async def execute_task(
        self, task: Task, run_id: str = "", **params: Any
    ) -> TaskRun:
        run_id = run_id or self.broker.new_uuid()
        task_run = self.create_task_run_from_task(task, run_id, **params)
        return await self.execute_task_run(task_run)

    async def execute_task_run(self, task_run: TaskRun) -> None:
        self.execute_async(task_run.wrapper())
        await task_run.waiter

    # process tasks from the internal queue
    async def _queue_task(self) -> None:
        try:
            queued_task = self._task_to_queue.pop()
        except IndexError:
            await asyncio.sleep(0.1)
        else:
            task_run = await self.broker.queue_task(self, queued_task)
            self.dispatch(task_run, "queued")
            await asyncio.sleep(0)

    async def _execute_async(self, message: AsyncMessage) -> None:
        try:
            executable, args = message
            await executable(*args)
        except Exception:
            logger.exception("unhandled exception while executing async callback")


class TaskConsumer(TaskManager):
    """The Task Consumer is responsible for consuming tasks from a task queue"""

    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
        self._concurrent_tasks: dict[str, dict[str, TaskRun]] = defaultdict(dict)
        self._priority_task_run_queue: deque[TaskRun] = deque()
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

    def num_concurrent_tasks_for(self, task_name: str) -> int:
        """The number of concurrent tasks for a given task_name"""
        return len(self._concurrent_tasks[task_name])

    async def queue_and_wait(self, task: str, **params: Any) -> Any:
        """Execute a task by-passing the broker task queue and wait for result"""
        return await self.execute(task, **params).waiter

    def execute(self, task: str, **params: Any) -> TaskRun:
        """Execute a Task by-passing the broker task queue"""
        queued_task = QueuedTask(
            run_id=self.broker.new_uuid(),
            task=task,
            params=params,
        )
        task_run = self.broker.create_task_run(self, queued_task)
        self._priority_task_run_queue.appendleft(task_run)
        return task_run

    # Internals
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
        task_run.start = utcnow()
        task_run.set_state(TaskState.running)
        self._concurrent_tasks[task_name][task_run.id] = task_run
        #
        if task_run.task.max_concurrency < self.num_concurrent_tasks_for(task_name):
            task_run.set_state(TaskState.rate_limited)
            task_run.waiter.set_result(None)
        elif not (await self.broker.get_tasks_info(task_name))[0].enabled:
            task_run.set_state(TaskState.aborted)
            task_run.waiter.set_result(None)
        #
        else:
            task_run.logger.info("start")
            self.dispatch(task_run, "start")
            try:
                result = await task_run.task.executor(task_run)
            except Exception as exc:
                task_run.waiter.set_exception(exc)
                task_run.set_state(TaskState.failure)
            else:
                if task_run.is_failure:
                    task_run.waiter.set_exception(TaskRunError())
                else:
                    task_run.waiter.set_result(result)
                    if not task_run.in_finish_state:
                        task_run.set_state(TaskState.success)
        try:
            await task_run.waiter
        except TaskRunError:
            # task_context.logger.warning("CPU bound task failure")
            # no need to log here, the log is already done from the CPU bound script
            pass
        except Exception:
            task_run.logger.exception("critical exception while executing")
        task_run.end = utcnow()
        self._concurrent_tasks[task_name].pop(task_run.id, None)
        await self.broker.update_task(
            task_run.task,
            dict(
                last_run_end=task_run.end,
                last_run_duration=task_run.duration,
                last_run_state=task_run.state,
            ),
        )
        self.dispatch(task_run, "end")
        task_run.logger.log(
            logging.WARNING if task_run.is_failure else logging.INFO,
            "end - %s - milliseconds - %s",
            task_run.state,
            round(task_run.duration.total_millis, 2),
        )


# required by pydantic to avoid `Class not fully defined` error
TaskRun.model_rebuild()
