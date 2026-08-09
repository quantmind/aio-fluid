from pydantic import BaseModel

from fluid.scheduler import TaskRun, TaskScheduler, task

SYMBOLS = ("BTC-USD", "ETH-USD")


class Symbol(BaseModel):
    symbol: str = "BTC-USD"


@task
async def daily_pipeline(ctx: TaskRun) -> None:
    """Start one chain per symbol."""
    for symbol in SYMBOLS:
        await ctx.queue(extract, symbol=symbol)


@task
async def extract(ctx: TaskRun[Symbol]) -> None:
    """Download the raw data, then hand over to the next step."""
    ctx.logger.info("extracting %s", ctx.params.symbol)
    # queue the next step and return, nothing blocks here.
    # if this task fails, transform is never queued
    await ctx.queue(transform, symbol=ctx.params.symbol)


@task
async def transform(ctx: TaskRun[Symbol]) -> None:
    """Normalise what extract downloaded."""
    ctx.logger.info(
        "transforming %s, chain started by %s", ctx.params.symbol, ctx.root_run_id
    )


def task_scheduler() -> TaskScheduler:
    scheduler = TaskScheduler()
    scheduler.register_from_dict(globals())
    return scheduler
