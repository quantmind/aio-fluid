import os

from .utils.text import to_bool

APP_NAME: str = os.getenv("APP_NAME", "fluid")
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "info").upper()
LOG_HANDLER = os.getenv("LOG_HANDLER", "plain")
PYTHON_LOG_FORMAT = os.getenv(
    "PYTHON_LOG_FORMAT",
    "%(asctime)s %(levelname)s %(name)s %(message)s",
)

# Workers
STOPPING_GRACE_PERIOD: int = int(os.getenv("FLUID_STOPPING_GRACE_PERIOD") or "10")
MAX_CONCURRENT_TASKS: int = int(os.getenv("FLUID_MAX_CONCURRENT_TASKS") or "5")
SCHEDULER_HEARTBEAT_MILLIS: int = int(
    os.getenv("FLUID_SCHEDULER_HEARTBEAT_MILLIS", "100")
)
BROKER_URL: str = os.getenv("FLUID_BROKER_URL", "redis://localhost:6379/3")


REDIS_DEFAULT_URL = os.getenv("REDIS_DEFAULT_URL", "redis://localhost:6379")
REDIS_MAX_CONNECTIONS = int(os.getenv("MAX_REDIS_CONNECTIONS", "5"))

# Database
DBPOOL_MAX_SIZE: int = int(os.getenv("FLUID_DBPOOL_MAX_SIZE") or "10")
DBPOOL_MAX_OVERFLOW: int = int(os.getenv("FLUID_DBPOOL_MAX_OVERFLOW") or "10")
DBECHO: bool = to_bool(os.getenv("FLUID_DBECHO") or "no")
