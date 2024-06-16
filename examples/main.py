from examples import tasks
from fluid.scheduler import TaskScheduler
from fluid.utils import log

task_manager = TaskScheduler()
task_manager.register_from_module(tasks)
task_manager_cli = task_manager.cli()


if __name__ == "__main__":
    log.config()
    task_manager_cli()
