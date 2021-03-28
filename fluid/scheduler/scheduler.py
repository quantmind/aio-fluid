import asyncio
from datetime import datetime
from typing import Dict

from ..node import Node
from .consumer import TaskManager
from .crontab import CronRun
from .task import Task


class TaskScheduler(TaskManager):
    """A task manager for scheduling tasks"""

    def __init__(self) -> None:
        super().__init__()
        self.add_workers(ScheduleTasks(self))


class ScheduleTasks(Node):
    heartbeat = 0.1

    def __init__(self, task_manager: TaskScheduler) -> None:
        super().__init__()
        self.task_manager: TaskScheduler = task_manager
        self.last_run: Dict[str, CronRun] = {}

    async def tick(self) -> None:
        now = datetime.utcnow()
        for task in self.task_manager.registry.periodic():
            run = task.schedule(now, self.last_run.get(task.name))
            if run:
                self.last_run[task.name] = run
                from_now = task.randomize() if task.randomize else 0
                if from_now:
                    asyncio.get_event_loop().call_later(from_now, self._queue, task)
                else:
                    self._queue(task)

    def _queue(self, task: Task) -> None:
        self.task_manager.queue(task)
