import asyncio
from datetime import timedelta

from fluid.scheduler import TaskContext, every, task


@task
async def dummy(context: TaskContext) -> None:
    await asyncio.sleep(0.1)
    if context.params.get("error"):
        raise RuntimeError("just an error")


@task(schedule=every(timedelta(seconds=1)))
async def scheduled(context: TaskContext) -> str:
    await asyncio.sleep(0.1)
    return "OK"
