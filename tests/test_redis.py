import aioredis
import pytest

from fluid.redis import ConnectionClosedError, RedisPubSub


async def close(conn: aioredis.Redis):
    conn.close()
    await conn.wait_closed()


async def test_redis_connection(redis: RedisPubSub):
    pub = await redis.pub()
    assert redis._pub._redis is pub


async def test_redis_connection_drop(redis: RedisPubSub):
    pub = await redis.pub()
    await pub.set("bla", "foo")
    await close(pub)
    with pytest.raises(ConnectionClosedError):
        await pub.set("bla", "foo2")
    foo = await redis.safe("get", "bla")
    assert foo == b"foo"
    pub = await redis.pub()
    await pub.set("bla", "foo2")


async def test_receiver(redis: RedisPubSub):
    def on_message(channel, message):
        pass

    receiver = redis.receiver(
        on_message=on_message, channels=["test"], patterns=["blaaa-*"]
    )
    await receiver.setup()
    pub = await redis.pub()
    channels = await pub.pubsub_channels()
    assert b"test" in channels
    await receiver.teardown()
    channels = await pub.pubsub_channels()
    assert b"test" not in channels
