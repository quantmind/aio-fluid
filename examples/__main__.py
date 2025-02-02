import dotenv

dotenv.load_dotenv()

from fluid.scheduler.cli import TaskManagerCLI

task_manager_cli = TaskManagerCLI(
    "examples.tasks:task_app", lazy_subcommands={"db": "examples.db.cli:cli"}
)


task_manager_cli()
