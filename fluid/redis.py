import asyncio
import os
from typing import Any, Callable, Dict, Optional, Sequence

import aioredis
from aioredis import ConnectionClosedError
from aioredis.pubsub import Receiver
from openapi import json

from .log import get_logger
from .node import NodeWorker

DEFAULT_URL = "redis://localhost:6379"
DEFAULT_CACHE_TIMEOUT = int(os.getenv("DEFAULT_CACHE_TIMEOUT", 300))
CACHE_KEY_PREFIX = os.getenv("CACHE_KEY_PREFIX", "cache")

logger = get_logger("redis")


def setup(app, redis_url: str = "", name: str = "", app_key="redis") -> None:
    redis = RedisPubSub(url=redis_url, name=name)
    app[app_key] = redis
    app.on_shutdown.append(redis.close)


class Connection:
    def __init__(self, url: str, name: str) -> None:
        self.url = url
        self.name = name
        self.lock = asyncio.Lock()
        self._redis = None

    async def get(self, *, connect: bool = True) -> Optional[aioredis.Redis]:
        if self._redis is None and connect:
            async with self.lock:
                self._redis = await self._connect()
        return self._redis

    async def safe(self, command: str, *args, **kwargs) -> Any:
        retried = False
        while True:
            redis = await self.get()
            try:
                return await getattr(redis, command)(*args, **kwargs)
            except ConnectionClosedError:
                self._redis = None
                if retried:
                    raise
                retried = True

    async def _connect(self) -> aioredis.Redis:
        redis = await aioredis.create_redis(self.url)
        await redis.client_setname(self.name)
        return redis

    async def close(self) -> None:
        async with self.lock:
            if self._redis:
                redis = self._redis
                self._redis = None
                redis.close()
                await redis.wait_closed()
                logger.warning("closed redis connections %s", self.name)


class MessageReceiver(NodeWorker):
    """Subscribe to a list of channels"""

    def __init__(
        self,
        sub: Connection,
        on_message: Callable[[str, str], None],
        channels: Sequence[str] = (),
        patterns: Sequence[str] = (),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sub = sub
        self.receiver = Receiver()
        self.on_message = on_message
        self.channels = [self.receiver.channel(channel) for channel in channels or ()]
        self.patterns = [self.receiver.pattern(pattern) for pattern in patterns or ()]
        assert self.channels or self.patterns, "No channels nor patterns specified"

    async def setup(self) -> None:
        redis = await self.sub.get()
        if self.channels:
            await redis.subscribe(*self.channels)
        if self.patterns:
            await redis.psubscribe(*self.patterns)

    async def work(self) -> None:
        async for channel, msg in self.receiver.iter():
            if self.is_running():
                self.on_message(channel, msg)
                await asyncio.sleep(0)
            else:
                break

    async def teardown(self) -> None:
        redis = await self.sub.get(connect=False)
        if redis:
            try:
                if self.channels:
                    await redis.unsubscribe(*self.channels)
                if self.patterns:
                    await redis.punsubscribe(*self.patterns)
            except (ConnectionClosedError, RuntimeError):
                pass
        self.receiver.stop()


class RedisPubSub:
    """Manage pubsub and queue"""

    def __init__(self, url: str = "", name: str = "") -> None:
        self.name = name or self.__class__.__name__
        self.url = url or os.getenv("REDIS_URL", DEFAULT_URL)
        self._lock = asyncio.Lock()
        self._pub = Connection(self.url, self.name)
        self._sub = Connection(self.url, f"{self.name}:streaming")
        self._pool: Optional[aioredis.Redis] = None

    async def pub(self, connect: bool = True) -> aioredis.Redis:
        return await self._pub.get()

    async def sub(self, connect: bool = True) -> aioredis.Redis:
        return await self._sub.get()

    async def pool(self) -> aioredis.Redis:
        async with self._lock:
            if self._pool is None:
                self._pool = await aioredis.create_redis_pool(self.url)
        return self._pool

    async def close(self, app=None) -> None:
        await asyncio.gather(self._pub.close(), self._sub.close())

    async def get_info(self) -> Dict:
        pub = await self.pub()
        return await pub.pubsub_channels()

    def receiver(
        self,
        on_message: Callable[[str, str], None],
        channels: Sequence[str] = None,
        patterns: Sequence[str] = None,
        **kwargs,
    ) -> MessageReceiver:
        return MessageReceiver(self._sub, on_message, channels, patterns, **kwargs)

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
        data = await self._pub.safe("get", cache_key)
        if not data and loader:
            expire = expire or DEFAULT_CACHE_TIMEOUT
            data = await loader(*args)
            if expire:
                await self._pub.safe("set", cache_key, json.dumps(data), expire=expire)
        elif data is not None:
            data = json.loads(data)
        return data

    async def invalidate_cache(self, key: str, *args) -> None:
        await self._pub.safe("del", self.cache_key(key, *args))

    # UTILITIES

    async def safe(self, command: str, *args, **kwargs) -> Any:
        return await self._pub.safe(command, *args, **kwargs)
