import asyncio
from typing import Callable


async def wait_for(assertion: Callable[[], bool], timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        while True:
            if assertion():
                return
            await asyncio.sleep(0)
