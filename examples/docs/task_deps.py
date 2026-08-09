from dataclasses import dataclass, field

from pydantic import BaseModel

from fluid.scheduler import TaskRun, TaskScheduler, task
from fluid.utils.http_client import HttpxClient


@dataclass
class Deps:
    """Dependencies shared by every task run."""

    http_client: HttpxClient = field(default_factory=HttpxClient)


class Quote(BaseModel):
    symbol: str = "BTC-USD"


@task
async def fetch_quote(ctx: TaskRun[Quote, Deps]) -> None:
    """Fetch a quote with the shared HTTP client."""
    data = await ctx.deps.http_client.get(
        f"https://api.example.com/quotes/{ctx.params.symbol}"
    )
    ctx.logger.info("got %s", data)


def task_scheduler() -> TaskScheduler:
    deps = Deps()
    scheduler = TaskScheduler(deps=deps)
    # the client is opened on startup and closed on shutdown
    scheduler.add_async_context_manager(deps.http_client)
    scheduler.register_from_dict(globals())
    return scheduler
