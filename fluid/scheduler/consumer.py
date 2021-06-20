import asyncio
from collections import defaultdict, deque
from functools import cached_property
from typing import Callable, Dict, NamedTuple, Optional, Union

from inflection import underscore

from fluid.node import Consumer, NodeWorkers, Worker
from fluid.utils import microseconds

from .broker import Broker, QueuedTask, TaskRegistry
from .constants import TaskPriority, TaskState
from .task import Task
from .task_run import TaskRun
from .utils import WaitFor

ConsumerCallback = Callable[[TaskRun, "TaskManager"], None]


class Event(NamedTuple):
    type: str
    tag: str

    @classmethod
    def from_string(cls, event: str) -> "Event":
        bits = event.split(".")
        return cls(bits[0], bits[1] if len(bits) > 1 else "")


class TaskManager(NodeWorkers):
    """Base class for both TaskConsumer and TaskScheduler"""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._msg_handlers: Dict[str, Dict[str, ConsumerCallback]] = defaultdict(dict)
        self._consumer = Consumer(
            self._execute_async, logger=self.logger.getChild("internal-consumer")
        )
        self._queue_tasks_worker = Worker(
            self._queue_tasks, logger=self.logger.getChild("queue")
        )
        self.add_workers(self._consumer)

    @cached_property
    def broker(self) -> Broker:
        return Broker.from_url()

    @property
    def registry(self) -> TaskRegistry:
        return self.broker.registry

    @cached_property
    def task_queue(self) -> asyncio.Queue:
        return asyncio.Queue()

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
        **params,
    ) -> QueuedTask:
        """Queue a Task for execution and return the QueuedTask object"""
        queued_task = QueuedTask(
            run_id=self.broker.new_uuid(),
            task=task,
            priority=priority,
            params=params,
        )
        self.task_queue.put_nowait(queued_task)
        return queued_task

    def dispatch(self, task_run: TaskRun, event_type: str) -> str:
        """Dispatch a message to the registered handlers.

        It returns the asset affected by the message (if any)
        """
        handlers = self._msg_handlers.get(event_type)
        if handlers:
            for handler in handlers.values():
                handler(task_run, self)

    def register_handler(self, event: str, handler: ConsumerCallback) -> None:
        event = Event.from_string(event)
        self._msg_handlers[event.type][event.tag] = handler

    def unregister_handler(self, event: str) -> None:
        event = Event.from_string(event)
        self._msg_handlers[event.type].pop(event.tag, None)

    def execute_async(self, async_callable, *args) -> None:
        self._consumer.submit((async_callable, args))

    # process tasks from the internal queue
    async def _queue_tasks(self) -> None:
        while self.is_running():
            queued_task = await self.task_queue.get()
            task_run = await self.broker.queue_task(queued_task)
            self.dispatch(task_run, "queued")
            await asyncio.sleep(0)

    async def _execute_async(self, message) -> None:
        try:
            executable, args = message
            await executable(*args)
        except Exception:
            self.logger.exception("unhandled exception while executing async callaback")


class ConsumerConfig(NamedTuple):
    max_concurrent_tasks: int = 5
    sleep: float = 0.2


class TaskConsumer(TaskManager):
    def __init__(self, **config) -> None:
        super().__init__()
        self.cfg: ConsumerConfig = ConsumerConfig(**config)
        self._concurrent_tasks: Dict[str, TaskRun] = {}
        self._priority_task_run_queue = deque()
        for i in range(self.cfg.max_concurrent_tasks):
            self.add_workers(
                Worker(
                    self._consume_tasks, logger=self.logger.getChild(f"worker-{i+1}")
                )
            )

    @property
    def num_concurrent_tasks(self) -> int:
        """The number of concurrent_tasks"""
        return len(self._concurrent_tasks)

    async def queue_and_wait(self, task: Union[str, Task], **params) -> TaskRun:
        run_id = self.execute(task, **params).id
        waitfor = WaitFor(run_id=run_id)
        self.register_handler(f"end.{waitfor.run_id}", waitfor)
        try:
            return await waitfor.waiter
        finally:
            self.unregister_handler(f"end.{waitfor.run_id}")

    def execute(self, task: Union[Task, str], **params) -> TaskRun:
        """Execute a Task by-passing the broker task queue"""
        queued_task = QueuedTask(
            run_id=self.broker.new_uuid(),
            task=task,
            priority="",
            params=params,
        )
        data = self.broker.task_run_data(queued_task, TaskState.queued)
        task_run = self.broker.task_run_from_data(data)
        self._priority_task_run_queue.appendleft(task_run)
        return task_run

    # Internals
    async def _consume_tasks(self) -> None:
        while self.is_running():
            if self._priority_task_run_queue:
                task_run = self._priority_task_run_queue.pop()
            else:
                task_run = await self.broker.get_task_run()
                if not task_run:
                    await asyncio.sleep(self.cfg.sleep)
                    continue
            task_run.start = microseconds()
            task_run.set_state(TaskState.running)
            # create task context
            task_context = task_run.task.create_context(self, task_run=task_run)
            task_context.logger.info("start")
            self._concurrent_tasks[task_run.id] = task_run
            self.dispatch(task_run, "start")
            try:
                result = await task_run.task.executor(task_context)
            except Exception as exc:
                task_run.waiter.set_exception(exc)
                task_run.set_state(TaskState.failure)
            else:
                task_run.waiter.set_result(result)
                if not task_run.in_finish_state:
                    task_run.set_state(TaskState.success)
            try:
                await task_run.waiter
            except Exception:
                task_context.logger.exception("critical exception while executing")
            task_run.end = microseconds()
            self._concurrent_tasks.pop(task_run.id)
            self.dispatch(task_run, "end")
            task_context.logger.info(
                "end - %s - milliseconds - %s",
                task_run.state,
                round(0.001 * task_run.duration, 3),
            )
            await asyncio.sleep(self.cfg.sleep)
