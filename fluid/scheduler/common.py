import os


def is_in_cpu_process() -> bool:
    return os.getenv("TASK_MANAGER_SPAWN") == "true"


def cpu_env() -> dict[str, str]:
    return dict(TASK_MANAGER_SPAWN="true")
