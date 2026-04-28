import asyncio
import inspect
from typing import Callable, Coroutine, Union

from typing_extensions import Annotated, Doc


async def wait_for(
    assertion: Annotated[
        Union[Callable[[], bool], Callable[[], Coroutine[object, object, bool]]],
        Doc(
            "The assertion to wait for. Can be a synchronous or asynchronous "
            "callable returning a boolean."
        ),
    ],
    timeout: Annotated[
        float,
        Doc("The maximum time to wait for the assertion to become true."),
    ] = 1.0,
) -> None:
    """Async Wait for an assertion to become true.
    If the assertion does not become true within the timeout,
    a TimeoutError is raised.
    """
    async with asyncio.timeout(timeout):
        while True:
            result = assertion()
            if inspect.isawaitable(result):
                result = await result
            if result:
                return
            await asyncio.sleep(0)
