import aioredis

from fluid.redis import RedisPubSub


async def close(conn: aioredis.Redis):
    conn.close()
    await conn.wait_closed()


async def test_receiver(redis: RedisPubSub):
    def on_message(channel, message):
        pass

    receiver = redis.receiver(
        on_message=on_message, channels=["test"], patterns=["blaaa-*"]
    )
    await receiver.setup()
    pub = await redis.pool()
    channels = await pub.pubsub_channels()
    assert b"test" in channels
    await receiver.teardown()
    channels = await pub.pubsub_channels()
    assert b"test" not in channels
