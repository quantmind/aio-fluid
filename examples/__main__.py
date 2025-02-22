import dotenv

dotenv.load_dotenv()

from examples.cli import task_manager_cli  # isort:skip    # noqa: E402


task_manager_cli()
