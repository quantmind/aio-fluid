from functools import cached_property
from typing import Dict

from ..node import Node, NodeWorkers
from .broker import Broker
from .task import Task


class Scheduler(NodeWorkers):
    def __init__(self) -> None:
        super().__init__()
        self.tasks = ScheduledTasks(self.broker)
        self.add_workers(self.tasks)

    @cached_property
    def broker(self) -> Broker:
        return Broker.from_env()

    async def teardown(self) -> None:
        await self.broker.close()

    async def queue(self, message, callback=True):
        """Queue the ``message``.
        If callback is True (default) returns a Future
        called back once the message is delivered,
        otherwise return a future called back once the messaged is queued
        """
        pass


class ScheduledTasks(Node):
    heartbeat = 10

    def __init__(self, broker: Broker) -> None:
        super().__init__()
        self.broker: Broker = broker
        self.loaded_tasks: Dict[str, Task] = {}

    async def tick(self) -> None:
        tasks = await self.broker.get_tasks()
        for name, task in tasks.items():
            new_task = Task(**task)
            old_task = self.loaded_tasks.get(name)
            if old_task != new_task:
                self.loaded_tasks[name] = new_task
                self.schedule(new_task, old_task)
