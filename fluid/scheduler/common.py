import os
from typing import Any

from pydantic import BaseModel

from fluid.utils.data import reveal_secrets


def params_dump(params: BaseModel) -> Any:
    """Dump task params with pydantic secret values revealed

    Task params are re-validated when a task run is consumed from the queue
    or executed in a subprocess, therefore secret values must be revealed
    rather than masked so they survive the round-trip.
    """
    return reveal_secrets(params.model_dump(mode="python"))


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
