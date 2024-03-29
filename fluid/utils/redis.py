import os
from typing import Any, Callable, Optional

from redis.asyncio import BlockingConnectionPool, Redis

import json
from fluid import settings

REDIS_DEFAULT_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DEFAULT_CACHE_TIMEOUT = int(os.getenv("DEFAULT_CACHE_TIMEOUT", 300))
REDIS_MAX_CONNECTIONS_DEFAULT = int(os.getenv("MAX_REDIS_CONNECTIONS", "5"))
CACHE_KEY_PREFIX = os.getenv("CACHE_KEY_PREFIX", "cache")

logger = settings.get_logger(__name__)


class FluidRedis:
    def __init__(
        self,
        url: str = "",
        name: str = "",
        max_connections: int = 0,
    ) -> None:
        self.cli: Redis = Redis(
            connection_pool=BlockingConnectionPool.from_url(
                url or REDIS_DEFAULT_URL,
                max_connections=max_connections or REDIS_MAX_CONNECTIONS_DEFAULT,
                client_name=name or settings.APP_NAME,
            )
        )

    async def close(self, app=None) -> None:
        await self.cli.close()
        await self.cli.connection_pool.disconnect()

    # CACHE UTILITIES

    def cache_key(self, key: str, *args) -> str:
        return "-".join(str(arg) for arg in (CACHE_KEY_PREFIX, key, *args))

    async def from_cache(
        self,
        key: str,
        *args,
        expire: int = 0,
        loader: Optional[Callable] = None,
    ) -> Any:
        """Load JSON data from redis cache"""
        cache_key = self.cache_key(key, *args)
        data = await self.cli.get(cache_key)
        if not data and loader:
            expire = expire or DEFAULT_CACHE_TIMEOUT
            data = await loader(*args)
            if expire:
                await self.cli.set(cache_key, json.dumps(data), ex=expire)
        elif data is not None:
            data = json.loads(data)
        return data

    async def invalidate_cache(self, key: str, *args) -> None:
        await self.cli.delete(self.cache_key(key, *args))
