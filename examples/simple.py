import asyncio

from examples.tasks import task_scheduler
from fluid.utils import log

if __name__ == "__main__":
    log.config()
    asyncio.run(task_scheduler().run())
