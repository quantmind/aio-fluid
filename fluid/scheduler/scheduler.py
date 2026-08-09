from typing import Any

from typing_extensions import Annotated, Doc

from fluid import settings
from fluid.utils.dates import utcnow
from fluid.utils.worker import WorkerFunction

from .consumer import TaskConsumer, TaskManagerConfig
from .scheduler_crontab import CronRun


class TaskScheduler(TaskConsumer):
    """A task manager for scheduling tasks"""

    def __init__(
        self,
        *,
        deps: Annotated[
            Any,
            Doc("""
                Application dependencies available to every task run.

                See the [Task Dependencies](../tutorials/task_deps.md)
                tutorial.
                """),
        ] = None,
        config: Annotated[
            TaskManagerConfig | None,
            Doc("""
                Task manager configuration.

                Built from the extra keyword arguments when not provided.
                """),
        ] = None,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
        stopping_grace_period: Annotated[
            float | None,
            Doc(
                "Grace period in seconds to wait for workers to stop running "
                "when this worker is shutdown. "
                "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
                "environment variable or 10 seconds."
            ),
        ] = None,
        **kwargs: Annotated[
            Any,
            Doc("Configuration fields, used when `config` is not provided."),
        ],
    ) -> None:
        super().__init__(
            deps=deps,
            config=config,
            name=name,
            stopping_grace_period=stopping_grace_period,
            **kwargs,
        )
        self.add_workers(ScheduleTasks(self))


class ScheduleTasks(WorkerFunction):
    def __init__(
        self,
        task_manager: TaskScheduler,
        heartbeat: float | int | None = None,
    ) -> None:
        if heartbeat is None:
            heartbeat = 0.001 * settings.SCHEDULER_HEARTBEAT_MILLIS
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
                    self.task_manager.sync_queue(task, delay=from_now)
