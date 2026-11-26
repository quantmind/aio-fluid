from logging import Logger

from pydantic import BaseModel

from fluid.scheduler import TaskRun, TaskScheduler, task
from fluid.utils import kernel


class Compress(BaseModel):
    path: str


class LogLines:
    """Send each line a process writes to a logger"""

    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        self.decoder = kernel.Utf8Decoder()

    def __call__(self, data: bytes) -> None:
        self.logger.info(self.decoder.decode(data).rstrip())


@task(timeout_seconds=300)
async def compress(ctx: TaskRun[Compress]) -> None:
    """Compress a file with gzip, keeping the original"""
    log = LogLines(ctx.logger)
    code = await kernel.run(
        "gzip",
        "--keep",
        "--force",
        "--verbose",
        ctx.params.path,
        result_callback=log,
        error_callback=log,
        stream_output=True,
        stream_error=True,
    )
    if code:
        raise RuntimeError(f"gzip exited with code {code}")


def task_scheduler() -> TaskScheduler:
    scheduler = TaskScheduler()
    scheduler.register_from_dict(globals())
    return scheduler
