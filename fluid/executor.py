import asyncio
from contextlib import contextmanager
from typing import Any, Callable
from unittest import mock


async def run(func: Callable, *args: Any) -> Any:
    return await asyncio.get_event_loop().run_in_executor(None, func, *args)


async def _in_thread_execution(func: Callable, *args: Any) -> Any:
    return func(*args)


@contextmanager
def mock_executor():
    """Mock executor so we can include in coverage which has concurrency
    set to greenlet
    """
    with mock.patch(
        "fluid.executor.run",
        _in_thread_execution,
    ):
        yield
