import os

SCHEDULER_BROKER_URL: str = os.getenv(
    "SCHEDULER_BROKER_URL", "redis://localhost:6379/3"
)

TASK_MANAGER_SPAWN: str = os.getenv("TASK_MANAGER_SPAWN", "")
