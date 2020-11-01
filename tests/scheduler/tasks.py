import asyncio
from datetime import datetime, timedelta

from fluid.scheduler import TaskContext, task


class Schedule:
    def __init__(self, delta: timedelta):
        self.delta = delta
        self._last_run = 0

    def __call__(self, now: datetime) -> bool:
        if not self._last_run or now - self._last_run > self.delta:
            self._last_run = now
            return True
        return False


@task
async def dummy(context: TaskContext) -> None:
    await asyncio.sleep(0.1)
    if context.params.get("error"):
        raise RuntimeError("just an error")


@task(schedule=Schedule(timedelta(seconds=1)))
async def scheduled(context: TaskContext) -> None:
    await asyncio.sleep(0.1)
    return "OK"
