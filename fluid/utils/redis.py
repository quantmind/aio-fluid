from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Self

from redis.asyncio import BlockingConnectionPool, Redis

from fluid import settings
from fluid.utils import log

logger = log.get_logger(__name__)


@dataclass
class FluidRedis:
    redis_cli: Redis

    @classmethod
    def create(
        cls,
        url: str = "",
        name: str = settings.APP_NAME,
        max_connections: int = settings.REDIS_MAX_CONNECTIONS,
    ) -> Self:
        return cls(
            redis_cli=Redis(
                connection_pool=BlockingConnectionPool.from_url(
                    url or settings.REDIS_DEFAULT_URL,
                    max_connections=max_connections,
                    client_name=name,
                ),
            ),
        )

    def __str__(self) -> str:
        kwargs = self.redis_cli.connection_pool.connection_kwargs
        return "%s:%s" % (kwargs.get("host"), kwargs.get("port", 6379))

    async def close(self, *args: Any) -> None:
        await self.redis_cli.aclose(True)  # type: ignore[attr-defined]
