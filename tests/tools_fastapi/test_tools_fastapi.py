from typing import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx2 import ASGITransport, AsyncClient

from fluid.tools_fastapi import (
    WorkersDep,
    app_workers,
    get_workers_from_app,
)
from fluid.tools_fastapi.service import FastapiAppWorkers

pytestmark = pytest.mark.asyncio(loop_scope="module")


def _create_app() -> FastAPI:
    app = FastAPI()

    @app.get("/workers")
    async def workers_info(workers: WorkersDep) -> dict:
        return {"workers": None if workers is None else workers.num_workers}

    return app


@pytest.fixture
async def client() -> AsyncIterator[tuple[FastAPI, AsyncClient]]:
    app = _create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as cli:
        yield app, cli


async def test_app_workers_is_cached() -> None:
    app = FastAPI()
    assert get_workers_from_app(app) is None
    workers = app_workers(app)
    assert isinstance(workers, FastapiAppWorkers)
    assert app.state.workers is workers
    assert app_workers(app) is workers
    assert workers.startup in app.router.on_startup
    assert workers.shutdown in app.router.on_shutdown


async def test_workers_dependency(client: tuple[FastAPI, AsyncClient]) -> None:
    app, cli = client
    response = await cli.get("/workers")
    assert response.status_code == 200
    assert response.json() == {"workers": None}
    app_workers(app)
    response = await cli.get("/workers")
    assert response.json() == {"workers": 0}


async def test_after_shutdown_in_test_env() -> None:
    """Outside the test environment this would SIGTERM the process."""
    workers = FastapiAppWorkers()
    workers.after_shutdown("normal shutdown", 0)
