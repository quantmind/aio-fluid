import os


def is_in_cpu_process() -> bool:
    """Check if the current process is a CPU process.

    A CPU process is a process that is spawned by the task manager to run
    a cpu-bound task.

    It is identified by the environment variable `TASK_MANAGER_SPAWN`
    being set to "true".
    """
    return os.getenv("TASK_MANAGER_SPAWN") == "true"


def cpu_env() -> dict[str, str]:
    """Get the environment variables for a CPU process."""
    return dict(TASK_MANAGER_SPAWN="true")
