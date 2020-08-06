from contextlib import asynccontextmanager

from aiohttp.test_utils import TestClient, TestServer
from aiohttp.web import Application
from openapi.json import dumps


@asynccontextmanager
async def app_cli(app: Application) -> TestClient:
    server = TestServer(app)
    client = TestClient(server, json_serialize=dumps)
    await client.start_server()
    try:
        yield client
    finally:
        await client.close()
