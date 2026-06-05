import os
from functools import lru_cache
from typing import Any, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_PREFIX = os.getenv("FLUID_ENV_PREFIX", "fluid_")


class Settings(BaseSettings):
    """Lazy application settings sourced from environment variables.

    Settings are read from the environment the first time
    [get_settings][fluid.settings.get_settings] is called, not at import time.
    Access the resolved values either via the cached instance or, for backwards
    compatibility, via upper-case module attributes (``settings.APP_NAME``),
    both of which resolve lazily.
    """

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
        env_prefix=ENV_PREFIX,
    )

    # Externally-named settings keep an explicit alias so they stay unprefixed.
    # Everything else resolves as ``{ENV_PREFIX}<field_name>`` (case-insensitive).
    app_name: str = Field(default="fluid", validation_alias="APP_NAME")
    env: str = Field(default="dev", validation_alias="PYTHON_ENV")
    log_level: str = Field(default="info", validation_alias="LOG_LEVEL")
    log_handler: str = Field(default="plain", validation_alias="LOG_HANDLER")
    python_log_format: str = Field(
        default="%(asctime)s %(levelname)s %(name)s %(message)s",
        validation_alias="PYTHON_LOG_FORMAT",
    )
    database: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/fluid",
        validation_alias="DATABASE",
    )
    redis_default_url: str = Field(
        default="redis://localhost:6379", validation_alias="REDIS_DEFAULT_URL"
    )

    # Workers
    stopping_grace_period: int = 10
    max_concurrent_tasks: int = 5
    sleep_millis: int = 1000
    scheduler_heartbeat_millis: int = 100
    broker_url: str = ""
    redis_max_connections: int = Field(
        default=5, validation_alias="MAX_REDIS_CONNECTIONS"
    )

    # Database
    database_schema: str | None = None
    dbpool_max_size: int = 10
    dbpool_max_overflow: int = 10
    dbecho: bool = False

    # HTTP
    http_user_agent: str = ""

    # Pagination
    default_pagination_limit: int = 250
    default_pagination_max_limit: int = 500

    # Console backdoor
    backdoor_port: int = 8087

    # Flamegraph
    flamegraph_executable: str = "flamegraph.pl"
    stack_sampler_period_seconds: int = 1

    @model_validator(mode="after")
    def _apply_derived_defaults(self) -> Self:
        self.log_level = self.log_level.upper()
        if not self.broker_url:
            self.broker_url = self.redis_default_url
        if not self.http_user_agent:
            self.http_user_agent = f"python/{self.app_name}"
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide [Settings][fluid.settings.Settings] instance.

    The instance is built on first call (reading the environment then) and
    cached for the lifetime of the process. Call ``get_settings.cache_clear()``
    to force a re-read, which is mostly useful in tests.
    """
    return Settings()


def __getattr__(name: str) -> Any:
    """Resolve legacy upper-case settings (e.g. ``settings.APP_NAME``) lazily.

    Dunder lookups are excluded so import machinery never instantiates the
    settings (and so never fills the cache) before the environment is ready.
    """
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)
    return getattr(get_settings(), name.lower())
