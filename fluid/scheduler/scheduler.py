import asyncio
from datetime import datetime

from ..node import Node
from .consumer import TaskManager
from .task import Task


class Scheduler(TaskManager):
    """A task manager for scheduling tasks"""

    def __init__(self) -> None:
        super().__init__()
        self.add_workers(ScheduledTasks(self))


class ScheduledTasks(Node):
    heartbeat = 0.1

    def __init__(self, task_manager: TaskManager) -> None:
        super().__init__()
        self.task_manager: TaskManager = task_manager

    async def tick(self) -> None:
        now = datetime.utcnow()
        for task in self.task_manager.registry.values():
            if task.schedule and task.schedule(now):
                from_now = task.randomize() if task.randomize else 0
                if from_now:
                    asyncio.get_event_loop().call_later(from_now, self._queue, task)
                else:
                    self._queue(task)

    def _queue(self, task: Task) -> None:
        self.task_manager.queue(task)
