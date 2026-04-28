import asyncio
import inspect
from typing import Callable, Coroutine, Union


async def wait_for(
    assertion: Union[Callable[[], bool], Callable[[], Coroutine[object, object, bool]]],
    timeout: float = 1.0,
) -> None:
    async with asyncio.timeout(timeout):
        while True:
            result = assertion()
            if inspect.isawaitable(result):
                result = await result
            if result:
                return
            await asyncio.sleep(0)
