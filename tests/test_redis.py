from fluid.redis import FluidRedis


async def test_receiver(redis: FluidRedis):
    def on_message(message):
        pass

    receiver = redis.receiver(
        on_message=on_message, channels=["test"], patterns=["blaaa-*"]
    )
    await receiver.setup()
    channels = await redis.cli.pubsub_channels()
    assert b"test" in channels
    await receiver.teardown()
    channels = await redis.cli.pubsub_channels()
    assert b"test" not in channels
