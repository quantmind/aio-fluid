import asyncio
import os

import pytest

os.environ["PYTHON_ENV"] = "test"


@pytest.fixture(scope="module", autouse=True)
def event_loop():
    """Return an instance of the event loop."""
    loop = asyncio.new_event_loop()
    try:
        yield loop
    finally:
        loop.close()
