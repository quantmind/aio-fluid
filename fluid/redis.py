import asyncio
import os
from typing import Any, Callable, Optional, Sequence

import async_timeout
from aioredis import Redis
from aioredis.client import PubSub

from . import json
from .log import APP_NAME, get_logger
from .node import NodeWorker

DEFAULT_URL = "redis://localhost:6379"
DEFAULT_CACHE_TIMEOUT = int(os.getenv("DEFAULT_CACHE_TIMEOUT", 300))
CACHE_KEY_PREFIX = os.getenv("CACHE_KEY_PREFIX", "cache")

logger = get_logger("redis")


def setup(
    app, redis_url: str = "", name: str = APP_NAME, app_key="redis"
) -> "FluidRedis":
    redis = FluidRedis(url=redis_url, name=name)
    app[app_key] = redis
    app.on_shutdown.append(redis.close)
    return redis


class MessageReceiver(NodeWorker):
    """Subscribe to a list of channels"""

    def __init__(
        self,
        sub: PubSub,
        on_message: Callable[[str, str], None],
        channels: Sequence[str] = (),
        patterns: Sequence[str] = (),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sub: PubSub = sub
        self.on_message = on_message
        self.channels = tuple(channels)
        self.patterns = tuple(patterns)

    async def setup(self) -> None:
        if self.channels:
            await self.sub.subscribe(*self.channels)
        if self.patterns:
            await self.sub.psubscribe(*self.patterns)

    async def work(self) -> None:
        while self.is_running():
            try:
                async with async_timeout.timeout(1):
                    message = self.sub.get_message(ignore_subscribe_messages=True)
                    if message is not None:
                        self.on_message(message)
                    await asyncio.sleep(0)
            except asyncio.TimeoutError:
                pass

    async def teardown(self) -> None:
        await self.sub.close()


class FluidRedis:
    def __init__(self, url: str = "", name: str = "") -> None:
        self.name = name or APP_NAME
        self.cli: Redis = Redis.from_url(url or os.getenv("REDIS_URL", DEFAULT_URL))

    async def close(self, app=None) -> None:
        await self.cli.close()
        await self.cli.connection_pool.disconnect()

    def receiver(
        self,
        on_message: Callable[[str, str], None],
        channels: Sequence[str] = (),
        patterns: Sequence[str] = (),
        **kwargs,
    ) -> MessageReceiver:
        return MessageReceiver(
            self.cli.pubsub(),
            on_message,
            channels,
            patterns,
            **kwargs,
        )

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
