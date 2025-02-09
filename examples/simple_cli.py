import dotenv

dotenv.load_dotenv()

from fluid.scheduler.cli import TaskManagerCLI  # isort:skip    # noqa: E402


if __name__ == "__main__":
    from examples.tasks import task_app

    task_manager_cli = TaskManagerCLI(task_app())
    task_manager_cli()
