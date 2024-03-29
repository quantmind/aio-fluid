from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Callable, Coroutine, Deque, Dict, NamedTuple, Optional

from inflection import underscore

from fluid.node import Consumer, NodeWorkers, Worker
from fluid.utils import microseconds

from .broker import Broker, QueuedTask, TaskRegistry, UnknownTask
from .constants import TaskPriority, TaskState
from .task import Task
from .task_run import TaskRun

ConsumerCallback = Callable[[TaskRun, "TaskManager"], None]
AsyncExecutor = Callable[..., Coroutine[Any, Any, None]]
AsyncMessage = tuple[AsyncExecutor, tuple[Any, ...]]


class TaskFailure(RuntimeError):
    pass


@dataclass
class TaskManagerConfig:
    schedule_tasks: bool = True
    consume_tasks: bool = True
    max_concurrent_tasks: int = 10
    """number of coroutine workers"""
    sleep: float = 0.1
    """amount to sleep after completion of a task"""
    broker_url: str = ""


class Event(NamedTuple):
    type: str
    tag: str

    @classmethod
    def from_string(cls, event: str) -> "Event":
        bits = event.split(".")
        return cls(bits[0], bits[1] if len(bits) > 1 else "")


class TaskManager(NodeWorkers):
    """Base class for both TaskConsumer and TaskScheduler"""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__()
        self._msg_handlers: Dict[str, Dict[str, ConsumerCallback]] = defaultdict(dict)
        self._task_to_queue: Deque[QueuedTask] = deque()
        self._queue_tasks_worker = Worker(
            self._queue_tasks, logger=self.logger.getChild("queue")
        )
        self._consumer = Consumer(
            self._execute_async, logger=self.logger.getChild("internal-consumer")
        )
        self.config: TaskManagerConfig = TaskManagerConfig(**kwargs)
        self.add_workers(self._consumer)

    @cached_property
    def broker(self) -> Broker:
        return Broker.from_url(self.config.broker_url)

    @property
    def registry(self) -> TaskRegistry:
        return self.broker.registry

    @property
    def type(self) -> str:
        return underscore(self.__class__.__name__)

    async def setup(self) -> None:
        await self._queue_tasks_worker.start_app(self.app)

    async def teardown(self) -> None:
        await self._queue_tasks_worker.close()
        await self.broker.close()

    def register_task(self, task: Task) -> None:
        """Register a task with the task manager

        Only tasks registered can be executed by a task manager
        """
        self.broker.register_task(task)

    def queue(
        self,
        task: str,
        priority: Optional[TaskPriority] = None,
        **params: Any,
    ) -> QueuedTask:
        """Queue a Task for execution and return schedule_tasksthe QueuedTask object"""
        queued_task = QueuedTask(
            run_id=self.broker.new_uuid(),
            task=task,
            priority=priority,
            params=params,
        )
        self._task_to_queue.appendleft(queued_task)
        return queued_task

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
        self._consumer.submit((async_callable, args))

    # process tasks from the internal queue
    async def _queue_tasks(self) -> None:
        while self.is_running():
            try:
                queued_task = self._task_to_queue.pop()
            except IndexError:
                await asyncio.sleep(0.1)
            else:
                task_run = await self.broker.queue_task(queued_task)
                self.dispatch(task_run, "queued")
                await asyncio.sleep(0)

    async def _execute_async(self, message: AsyncMessage) -> None:
        try:
            executable, args = message
            await executable(*args)
        except Exception:
            self.logger.exception("unhandled exception while executing async callaback")


class TaskConsumer(TaskManager):
    """The Task Consumer is responsible for consuming tasks from a task queue"""

    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
        self._concurrent_tasks: dict[str, dict[str, TaskRun]] = defaultdict(dict)
        self._priority_task_run_queue: deque[TaskRun] = deque()
        for i in range(self.config.max_concurrent_tasks):
            self.add_workers(
                Worker(
                    self._consume_tasks, logger=self.logger.getChild(f"worker-{i+1}")
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
        data = self.broker.task_run_data(queued_task, TaskState.queued)
        task_run = self.broker.task_run_from_data(data)
        self._priority_task_run_queue.appendleft(task_run)
        return task_run

    # Internals
    async def _consume_tasks(self) -> None:
        while self.is_running():
            if not self.config.consume_tasks:
                await asyncio.sleep(self.config.sleep)
                continue
            if self._priority_task_run_queue:
                task_run = self._priority_task_run_queue.pop()
            else:
                try:
                    maybe_task_run = await self.broker.get_task_run()
                except UnknownTask as exc:
                    self.logger.error(
                        "unknown task %s - it looks like it is not "
                        "registered with this consumer",
                        exc,
                    )
                    maybe_task_run = None
                if not maybe_task_run:
                    await asyncio.sleep(self.config.sleep)
                    continue
                else:
                    task_run = maybe_task_run
            task_name = task_run.name
            task_run.start = microseconds()
            task_run.set_state(TaskState.running)
            task_context = task_run.task.create_context(self, task_run=task_run)
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
                task_context.logger.info("start")
                self.dispatch(task_run, "start")
                try:
                    result = await task_run.task.executor(task_context)
                except Exception as exc:
                    task_run.waiter.set_exception(exc)
                    task_run.set_state(TaskState.failure)
                else:
                    if task_run.is_failure:
                        task_run.waiter.set_exception(TaskFailure())
                    else:
                        task_run.waiter.set_result(result)
                        if not task_run.in_finish_state:
                            task_run.set_state(TaskState.success)
            try:
                await task_run.waiter
            except TaskFailure:
                # task_context.logger.warning("CPU bound task failure")
                # no need to log here, the log is already done from the CPU bound script
                pass
            except Exception:
                task_context.logger.exception("critical exception while executing")
            task_run.end = microseconds()
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
            task_context.logger.log(
                logging.WARNING if task_run.is_failure else logging.INFO,
                "end - %s - milliseconds - %s",
                task_run.state,
                round(0.001 * task_run.duration, 3),
            )
            await asyncio.sleep(0)
