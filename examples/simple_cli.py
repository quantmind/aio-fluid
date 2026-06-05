import dotenv

dotenv.load_dotenv()


if __name__ == "__main__":
    from examples.tasks import task_app
    from fluid.scheduler.cli import TaskManagerCLI

    task_manager_cli = TaskManagerCLI(
        task_app(),
        help="Simple Task Manager CLI with default commands",
    )
    task_manager_cli()
