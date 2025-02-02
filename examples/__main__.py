import dotenv

dotenv.load_dotenv()

from fluid.scheduler.cli import TaskManagerCLI  # isort:skip    # noqa: E402

task_manager_cli = TaskManagerCLI(
    "examples.tasks:task_app", lazy_subcommands={"db": "examples.db.cli:cli"}
)


task_manager_cli()
