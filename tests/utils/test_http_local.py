from typing import AsyncIterator, Callable

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from prometheus_client import REGISTRY

from fluid.utils.http_client import (
    AioHttpClient,
    HttpClient,
    HttpResponseError,
    HttpxClient,
)

pytestmark = pytest.mark.asyncio(loop_scope="module")

CLIENT_CLASSES = [HttpxClient, AioHttpClient]


async def _bytes(request: web.Request) -> web.Response:
    return web.Response(body=b"\x00\x01binary", content_type="application/octet-stream")


async def _bad_json(request: web.Request) -> web.Response:
    return web.Response(status=500, text="not json", content_type="application/json")


async def _json(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def _local_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/bytes", _bytes)
    app.router.add_get("/bad-json", _bad_json)
    app.router.add_get("/items/{item_id}", _json)
    return app


@pytest.fixture
async def server() -> AsyncIterator[TestServer]:
    """A real HTTP server bound to localhost on a random port."""
    srv = TestServer(_local_app())
    async with srv:
        yield srv


@pytest.fixture(params=CLIENT_CLASSES, ids=lambda cls: cls.__name__)
def client_class(request: pytest.FixtureRequest) -> Callable[..., HttpClient]:
    return request.param


async def test_bytes_response(
    server: TestServer, client_class: Callable[..., HttpClient]
) -> None:
    """Content which is neither json nor text is returned as bytes."""
    async with client_class() as client:
        data = await client.get(str(server.make_url("/bytes")))
    assert data == b"\x00\x01binary"


async def test_bytes_response_with_callback(
    server: TestServer, client_class: Callable[..., HttpClient]
) -> None:
    async with client_class() as client:
        response = await client.get(str(server.make_url("/bytes")), callback=True)
        assert await response.bytes() == b"\x00\x01binary"


async def test_error_with_unparsable_body(
    server: TestServer, client_class: Callable[..., HttpClient]
) -> None:
    """An error body which cannot be decoded falls back to the raw text."""
    url = str(server.make_url("/bad-json"))
    async with client_class() as client:
        with pytest.raises(HttpResponseError) as exc_info:
            await client.get(url)
    error = exc_info.value
    assert error.status_code == 500
    assert error.data["response"] == {"message": "not json"}
    assert error.data["request_url"] == url
    assert error.data["request_method"] == "GET"
    text = str(error)
    assert '"message": "not json"' in text
    assert '"response_status": 500' in text


async def test_monitor_http(
    server: TestServer, client_class: Callable[..., HttpClient]
) -> None:
    """Requests are timed in the prometheus histogram with a sanitized path."""
    url = server.make_url("/items/123")
    path = f"/items/{{id}}-{client_class.__name__}"
    labels = dict(host=url.authority, path=path, method="GET", status="200")
    before = (
        REGISTRY.get_sample_value("http_request_duration_seconds_count", labels) or 0
    )
    async with client_class() as client:
        data = await client.get(str(url), monitor_http=lambda p: path)
    assert data == {"ok": True}
    after = REGISTRY.get_sample_value("http_request_duration_seconds_count", labels)
    assert after == before + 1
