import asyncio
from datetime import datetime, timedelta

from fluid.scheduler import every


async def test_delay():
    scheduler = every(timedelta(seconds=10), delay=timedelta(seconds=0.3))
    assert scheduler(datetime.now()) is None
    await asyncio.sleep(0.4)
    assert scheduler(datetime.now()) is not None
