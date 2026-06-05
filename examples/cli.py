import dotenv

from fluid.scheduler.cli import TaskManagerCLI

dotenv.load_dotenv()

task_manager_cli = TaskManagerCLI(
    "examples.tasks:task_app", lazy_subcommands={"db": "examples.db.cli:cli"}
)
