import asyncio
from dataclasses import dataclass, field

from .broker import TaskRun


@dataclass
class WaitFor:
    run_id: str
    waiter: asyncio.Future = field(default_factory=asyncio.Future)

    def __call__(self, task_run: TaskRun, _):
        if task_run.id == self.run_id:
            self.waiter.set_result(task_run)
