import asyncio
from collections import defaultdict, deque
from functools import cached_property
from typing import Callable, Dict, NamedTuple, Union

from fluid.node import NodeWorkers, Worker
from fluid.utils import milliseconds

from .broker import Broker, TaskRun
from .task import Task

ConsumerCallback = Callable[[TaskRun], None]


class Event(NamedTuple):
    type: str
    tag: str

    @classmethod
    def from_string(cls, event: str) -> "Event":
        bits = event.split(".")
        return cls(bits[0], bits[1] if len(bits) > 1 else "")


class TaskManager(NodeWorkers):
    def __init__(self) -> None:
        super().__init__()
        self._msg_handlers: Dict[str, Dict[str, ConsumerCallback]] = defaultdict(dict)
        self.add_workers(Worker(self._queue_tasks))

    @cached_property
    def broker(self) -> Broker:
        return Broker.from_env()

    @cached_property
    def task_queue(self) -> asyncio.Queue:
        return asyncio.Queue()

    async def teardown(self) -> None:
        await self.broker.close()

    def register_task(self, task: Task) -> None:
        self.broker.register_task(task)

    def queue(self, task: Union[str, Task], **params):
        """Queue a Task for execution
        """
        self.task_queue.put_nowait((task, params))

    def dispatch(self, task_run: TaskRun, event_type: str) -> str:
        """Dispatch a message to the registered handlers.

        It returns the asset affected by the message (if any)
        """
        handlers = self._msg_handlers.get(event_type)
        if handlers:
            for handler in handlers.values():
                handler(task_run)

    def register_handler(self, event: str, handler: ConsumerCallback) -> None:
        event = Event.from_string(event)
        self._msg_handlers[event.type][event.tag] = handler

    def unregister_handler(self, event: str) -> None:
        event = Event.from_string(event)
        self._msg_handlers[event.type].pop(event.tag, None)

    # process tasks from the internal queue
    async def _queue_tasks(self) -> None:
        while self.is_running():
            task, params = await self.task_queue.get()
            task_run = await self.broker.queue_task(task, params)
            self.dispatch(task_run, "queued")


class ConsumerConfig(NamedTuple):
    max_concurrent_tasks: int = 5


class Consumer(TaskManager):
    def __init__(self, **config) -> None:
        super().__init__()
        self.cfg: ConsumerConfig = ConsumerConfig(**config)
        self._concurrent_tasks: Dict[str, TaskRun] = {}
        self._priority_task_run_queue = deque()
        for _ in range(self.cfg.max_concurrent_tasks):
            self.add_workers(Worker(self._consume_tasks))

    @property
    def num_concurrent_tasks(self) -> int:
        """The number of concurrent_tasks
        """
        return len(self._concurrent_tasks)

    def execute(self, task: Union[Task, str], **params) -> TaskRun:
        """Execute a Task by-passing the broker task queue"""
        data = self.broker.task_run_data(task, params)
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
                    continue
            task_run.start = milliseconds()
            self._concurrent_tasks[task_run.id] = task_run
            self.dispatch(task_run, "start")
            try:
                result = await task_run.task(self)
            except Exception as exc:
                task_run.result.set_exception(exc)
            else:
                task_run.result.set_result(result)
            try:
                await task_run.result
            except Exception:
                self.logger.exception("Critical exception in task %s", task_run.name)
            task_run.end = milliseconds()
            self._concurrent_tasks.pop(task_run.id)
            self.dispatch(task_run, "end")
