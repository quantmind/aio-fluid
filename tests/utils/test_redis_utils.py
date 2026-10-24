import pytest

from fluid import settings
from fluid.utils.redis import FluidRedis

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_create_defaults() -> None:
    redis = FluidRedis.create()
    try:
        pool = redis.redis_cli.connection_pool
        assert pool.max_connections == settings.REDIS_MAX_CONNECTIONS
        assert pool.connection_kwargs["client_name"] == settings.APP_NAME
        assert await redis.redis_cli.ping()
    finally:
        await redis.close()


async def test_create_custom_and_str() -> None:
    redis = FluidRedis.create(
        url="redis://localhost:6379/3", name="custom", max_connections=2
    )
    try:
        pool = redis.redis_cli.connection_pool
        assert pool.max_connections == 2
        assert pool.connection_kwargs["client_name"] == "custom"
        assert str(redis) == "localhost:6379/3"
        assert await redis.redis_cli.client_getname() == "custom"
    finally:
        await redis.close()
