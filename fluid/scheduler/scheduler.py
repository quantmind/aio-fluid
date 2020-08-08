from typing import Dict

from ..node import Node
from .broker import Broker
from .consumer import TaskManager
from .task import Task


class Scheduler(TaskManager):
    def __init__(self) -> None:
        super().__init__()
        self.tasks = ScheduledTasks(self.broker)
        self.add_workers(self.tasks)


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
