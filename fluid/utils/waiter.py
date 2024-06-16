import asyncio
from typing import Callable

import async_timeout


async def wait_for(assertion: Callable[[], bool], timeout: float = 1.0) -> None:
    async with async_timeout.timeout(timeout):
        while True:
            if assertion():
                return
            await asyncio.sleep(0)
