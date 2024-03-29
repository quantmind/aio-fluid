import asyncio
import json
import sys
from importlib import import_module

from fluid import settings
from fluid.scheduler import TaskManager

logger = settings.get_logger(__name__)


def create_task_manager() -> TaskManager:
    return TaskManager()


async def main(name: str, module_name: str, run_id: str, params: str) -> int:
    task_manager = TaskManager()
    try:
        module = import_module(module_name)
        task = getattr(module, name)
        kwargs = json.loads(params)
        task_manager = create_task_manager()
        await task_manager.startup()
        await task_manager.execute_task(task, run_id=run_id, **kwargs)
    except Exception:
        logger.exception("Unhandled exception executing CPU task")
        return 1
    finally:
        await task_manager.on_shutdown()
    return 0


if __name__ == "__main__":
    exit(asyncio.get_event_loop().run_until_complete(main(*sys.argv[1:])))
