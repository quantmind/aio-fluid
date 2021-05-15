import asyncio
from collections import defaultdict, deque
from functools import cached_property
from typing import Callable, Dict, NamedTuple, Union

from inflection import underscore

from fluid.node import Consumer, NodeWorkers, Worker
from fluid.utils import milliseconds

from .broker import Broker, TaskRegistry, TaskRun
from .task import Task
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
        return Broker.from_env()

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

    def queue(self, task: Union[str, Task], **params) -> str:
        """Queue a Task for execution and return the run id"""
        run_id = self.broker.new_uuid()
        self.task_queue.put_nowait((run_id, task, params))
        return run_id

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
            run_id, task, params = await self.task_queue.get()
            task_run = await self.broker.queue_task(run_id, task, params)
            self.dispatch(task_run, "queued")
            await asyncio.sleep(0)

    async def _execute_async(self, message) -> None:
        try:
            executable, args = message
            await executable(*args)
        except Exception:
            self.logger.exception("unhadled exception while executing async callaback")


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
        data = self.broker.task_run_data(self.broker.new_uuid(), task, params)
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
            task_run.start = milliseconds()
            self.logger.info("start task.%s", task_run.name_id)
            self._concurrent_tasks[task_run.id] = task_run
            self.dispatch(task_run, "start")
            try:
                params = {**task_run.params, "run_id": task_run.id}
                result = await task_run.task(self, **params)
            except Exception as exc:
                task_run.waiter.set_exception(exc)
            else:
                task_run.waiter.set_result(result)
            try:
                await task_run.waiter
            except Exception:
                self.logger.exception("Critical exception in task %s", task_run.name)
            task_run.end = milliseconds()
            self._concurrent_tasks.pop(task_run.id)
            self.dispatch(task_run, "end")
            self.logger.info(
                "end task.%s in %s milliseconds", task_run.name_id, task_run.end
            )
            await asyncio.sleep(self.cfg.sleep)
