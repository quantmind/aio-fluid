import asyncio
import json
import sys
from importlib import import_module

from fluid.scheduler import TaskManager
from fluid.utils import log

logger = log.get_logger(__name__)


def create_task_manager() -> TaskManager:
    return TaskManager()


async def main(name: str, module_name: str, run_id: str, params: str) -> None:
    log.config()
    module = import_module(module_name)
    task = getattr(module, name)
    kwargs = json.loads(params)
    async with create_task_manager() as task_manager:
        await task_manager.execute(task, run_id=run_id, **kwargs)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(*sys.argv[1:]))
