import asyncio
from dataclasses import dataclass, field
from typing import Any

from .task_run import TaskRun


@dataclass
class WaitFor:
    run_id: str
    waiter: asyncio.Future[Any] = field(default_factory=asyncio.Future)

    def __call__(self, task_run: TaskRun, _: Any) -> None:
        if task_run.id == self.run_id:
            self.waiter.set_result(task_run)
