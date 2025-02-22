import asyncio
import json
import sys
from importlib import import_module

from fluid.scheduler import TaskManager
from fluid.utils import log


async def main(name: str, module_name: str, run_id: str, params: str) -> None:
    log.config()
    module = import_module(module_name)
    task = getattr(module, name)
    kwargs = json.loads(params)
    async with TaskManager() as task_manager:
        await task_manager.execute(task, run_id=run_id, **kwargs)


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
