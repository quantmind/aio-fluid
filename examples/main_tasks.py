from fluid.scheduler.cli import TaskManagerCLI

task_manager_cli = TaskManagerCLI("examples.tasks:task_app")


if __name__ == "__main__":
    task_manager_cli()
