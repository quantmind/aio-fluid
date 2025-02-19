import os


def is_in_subprocess() -> bool:
    return os.getenv("TASK_MANAGER_SPAWN") == "true"
