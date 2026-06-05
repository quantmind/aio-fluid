from pathlib import Path

import dotenv

from fluid.db import CrudDB
from fluid.db.cli import DbGroup
from fluid.scheduler.cli import DEFAULT_COMMANDS, TaskManagerCLI
from fluid.scheduler.db import TaskDbPlugin

dotenv.load_dotenv()

MIGRATIONS_PATH = Path(__file__).parent / "migrations"


def create_cli() -> TaskManagerCLI:
    from examples.tasks import task_app

    # create the database
    db = CrudDB.from_env(migration_path=MIGRATIONS_PATH, db_name="fluid_full_cli")
    # create the client
    task_manager_cli = TaskManagerCLI(
        task_app(plugins=[TaskDbPlugin(db)]),
        commands=list(DEFAULT_COMMANDS) + [DbGroup(db)],
        help="Task Manager CLI with db plugin",
    )
    return task_manager_cli


if __name__ == "__main__":
    task_manager_cli = create_cli()
    task_manager_cli()
