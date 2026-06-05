import asyncio

import dotenv

from fluid.utils import log

dotenv.load_dotenv()

if __name__ == "__main__":
    from examples.tasks import task_scheduler

    log.config()
    asyncio.run(task_scheduler().run())
