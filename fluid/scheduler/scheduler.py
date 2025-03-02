import asyncio
from typing import Any

from fluid import settings
from fluid.utils.dates import utcnow
from fluid.utils.worker import WorkerFunction

from .consumer import TaskConsumer
from .scheduler_crontab import CronRun


class TaskScheduler(TaskConsumer):
    """A task manager for scheduling tasks"""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.add_workers(ScheduleTasks(self))


class ScheduleTasks(WorkerFunction):
    def __init__(
        self,
        task_manager: TaskScheduler,
        heartbeat: float | int = 0.001 * settings.SCHEDULER_HEARTBEAT_MILLIS,
    ) -> None:
        super().__init__(self.tick, heartbeat=heartbeat)
        self.task_manager: TaskScheduler = task_manager
        self.last_run: dict[str, CronRun] = {}

    async def tick(self) -> None:
        if not self.task_manager.config.schedule_tasks:
            return
        now = utcnow()
        periodic_tasks = await self.task_manager.broker.filter_tasks(
            scheduled=True, enabled=True
        )
        for task in periodic_tasks:
            if task.schedule:
                run = task.schedule(now, self.last_run.get(task.name))
                if run:
                    self.last_run[task.name] = run
                    from_now = task.randomize() if task.randomize else 0
                    asyncio.get_event_loop().call_later(
                        from_now, self.task_manager.sync_queue, task
                    )
