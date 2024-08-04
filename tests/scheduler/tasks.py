from examples import tasks
from fluid.scheduler import TaskManager


def task_application(manager: TaskManager | None = None) -> TaskManager:
    if manager is None:
        manager = TaskManager()
    manager.register_from_module(tasks)
    return manager
