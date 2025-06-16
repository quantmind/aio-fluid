import os

from .utils.text import to_bool

APP_NAME: str = os.getenv("APP_NAME", "fluid")
ENV: str = os.getenv("PYTHON_ENV", "dev")
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "info").upper()
LOG_HANDLER = os.getenv("LOG_HANDLER", "plain")
PYTHON_LOG_FORMAT = os.getenv(
    "PYTHON_LOG_FORMAT",
    "%(asctime)s %(levelname)s %(name)s %(message)s",
)

# Workers
STOPPING_GRACE_PERIOD: int = int(os.getenv("FLUID_STOPPING_GRACE_PERIOD") or "10")
MAX_CONCURRENT_TASKS: int = int(os.getenv("FLUID_MAX_CONCURRENT_TASKS") or "5")
SLEEP_MILLIS: int = int(os.getenv("FLUID_SLEEP_MILLIS") or "1000")
SCHEDULER_HEARTBEAT_MILLIS: int = int(
    os.getenv("FLUID_SCHEDULER_HEARTBEAT_MILLIS", "100")
)
REDIS_DEFAULT_URL = os.getenv("REDIS_DEFAULT_URL", "redis://localhost:6379")
BROKER_URL: str = os.getenv("FLUID_BROKER_URL", REDIS_DEFAULT_URL)
REDIS_MAX_CONNECTIONS = int(os.getenv("MAX_REDIS_CONNECTIONS", "5"))

# Database
DATABASE = os.getenv(
    "DATABASE", "postgresql+asyncpg://postgres:postgres@localhost:5432/fluid"
)
DATABASE_SCHEMA: str | None = os.getenv("DATABASE_SCHEMA")
DBPOOL_MAX_SIZE: int = int(os.getenv("FLUID_DBPOOL_MAX_SIZE") or "10")
DBPOOL_MAX_OVERFLOW: int = int(os.getenv("FLUID_DBPOOL_MAX_OVERFLOW") or "10")
DBECHO: bool = to_bool(os.getenv("FLUID_DBECHO") or "no")

# HTTP
HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", f"python/{APP_NAME}")

# Pagination
DEFAULT_PAGINATION_LIMIT = int(os.getenv("DEFAULT_PAGINATION_LIMIT") or "250")
DEFAULT_PAGINATION_MAX_LIMIT = int(os.getenv("DEFAULT_PAGINATION_MAX_LIMIT") or "500")

# Console backdoor
AIO_BACKDOOR_PORT: int = int(os.environ.get("AIO_BACKDOOR_PORT", "8087"))

# Flamegraph
FLAMEGRAPH_EXECUTABLE: str = os.getenv("FLUID_FLAMEGRAPH_EXECUTABLE", "flamegraph.pl")
STACK_SAMPLER_PERIOD_SECONDS: int = int(
    os.getenv("FLUID_STACK_SAMPLER_PERIOD_SECONDS", "1")
)
