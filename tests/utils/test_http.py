from typing import Any, Awaitable, Callable

import aiohttp
import httpx2 as httpx
import pytest

from fluid.utils.http_client import (
    AioHttpClient,
    HttpClient,
    HttpResponse,
    HttpResponseError,
    HttpxClient,
    HttpxResponse,
)

# The behavioural tests below run against every concrete client implementation.
CLIENT_CLASSES = [HttpxClient, AioHttpClient]


@pytest.fixture(params=CLIENT_CLASSES, ids=lambda cls: cls.__name__)
def client_class(request: pytest.FixtureRequest) -> Callable[..., HttpClient]:
    """Parametrized client factory, so each test runs for httpx and aiohttp."""
    return request.param


async def _request_or_skip(coro: Awaitable[Any]) -> Any:
    """Await an httpbin.org request, skipping the test when it is unavailable.

    httpbin.org is an external service, so transient gateway errors (>= 502)
    and transport/timeout failures skip rather than fail the suite. This covers
    both ways a gateway error surfaces: raised (no callback) or returned as the
    response object (``callback=True``).
    """
    try:
        result = await coro
    except HttpResponseError as exc:
        if exc.status_code >= 502:
            pytest.skip(f"httpbin.org unavailable ({exc.status_code})")
        raise
    except (httpx.HTTPError, aiohttp.ClientError, OSError) as exc:
        pytest.skip(f"httpbin.org unreachable: {exc!r}")
    if isinstance(result, HttpResponse) and result.status_code >= 502:
        pytest.skip(f"httpbin.org unavailable ({result.status_code})")
    return result


def _mock_response(
    status_code: int = 200,
    json_data: dict | None = None,
    text_data: str | None = None,
    content: bytes | None = None,
    method: str = "GET",
    url: str = "https://example.com/api",
    headers: dict | None = None,
) -> httpx.Response:
    """Build a synthetic httpx.Response for testing."""
    if headers is None:
        headers = {"content-type": "application/json"}
    request = httpx.Request(method, url)
    return httpx.Response(
        status_code,
        json=json_data,
        text=text_data,
        content=content,
        request=request,
        headers=headers,
    )


# ---------------------------------------------------------------------------
# HttpxResponse
# ---------------------------------------------------------------------------


async def test_httpx_response_wrap():
    inner = _mock_response(
        status_code=200,
        json_data={"key": "value"},
        method="POST",
        url="https://example.com/api",
    )
    wrapped = HttpxResponse(inner)
    assert wrapped.url == "https://example.com/api"
    assert wrapped.status_code == 200
    assert wrapped.method == "POST"
    assert wrapped.headers == inner.headers


async def test_httpx_response_json():
    inner = _mock_response(json_data={"a": 1})
    wrapped = HttpxResponse(inner)
    data = await wrapped.json()
    assert data == {"a": 1}


async def test_httpx_response_text():
    inner = _mock_response(text_data="hello world")
    wrapped = HttpxResponse(inner)
    text = await wrapped.text()
    assert text == "hello world"


async def test_httpx_response_bytes():
    inner = _mock_response(content=b"binary-data")
    wrapped = HttpxResponse(inner)
    raw = await wrapped.bytes()
    assert raw == b"binary-data"


# ---------------------------------------------------------------------------
# HttpClient – basic HTTP methods (httpx and aiohttp)
# ---------------------------------------------------------------------------


async def test_client_get(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        response = await _request_or_skip(
            client.get("https://httpbin.org/get", callback=True)
        )
        assert response.status_code == 200
        data = await response.json()
        assert data["url"] == "https://httpbin.org/get"
        assert response.headers["Content-Type"] == "application/json"
        assert response.method == "GET"
        assert response.url == "https://httpbin.org/get"


async def test_client_post(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        response = await _request_or_skip(
            client.post(
                "https://httpbin.org/post",
                json={"hello": "world"},
                callback=True,
            )
        )
        assert response.status_code == 200
        data = await response.json()
        assert data["json"] == {"hello": "world"}


async def test_client_put(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        response = await _request_or_skip(
            client.put(
                "https://httpbin.org/put",
                json={"k": "v"},
                callback=True,
            )
        )
        assert response.status_code == 200
        data = await response.json()
        assert data["json"] == {"k": "v"}


async def test_client_patch(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        response = await _request_or_skip(
            client.patch(
                "https://httpbin.org/patch",
                json={"k": "v"},
                callback=True,
            )
        )
        assert response.status_code == 200
        data = await response.json()
        assert data["json"] == {"k": "v"}


async def test_client_delete(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        response = await _request_or_skip(
            client.delete("https://httpbin.org/delete", callback=True)
        )
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# HttpClient – session management
# ---------------------------------------------------------------------------


async def test_client_creates_own_session(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        # session is created lazily on first request
        assert client.session is None
        assert client.session_owner is False
        await _request_or_skip(client.get("https://httpbin.org/get", callback=True))
        assert client.session is not None
        assert client.session_owner is True


async def test_client_close_twice_is_safe(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        pass
    # already closed by __aexit__ – calling again should not raise
    await client.close()


async def test_httpx_client_explicit_session():
    async with httpx.AsyncClient() as session:
        client = HttpxClient(session=session)
        assert client.session is session
        assert client.session_owner is False
        await client.close()
        # close() only clears session when session_owner is True
        assert client.session is session
        # outer session should still be usable
        assert not session.is_closed


async def test_aiohttp_client_explicit_session():
    async with aiohttp.ClientSession() as session:
        client = AioHttpClient(session=session)
        assert client.session is session
        assert client.session_owner is False
        await client.close()
        # close() only clears session when session_owner is True
        assert client.session is session
        # outer session should still be usable
        assert not session.closed


# ---------------------------------------------------------------------------
# HttpClient – response data
# ---------------------------------------------------------------------------


async def test_client_auto_parse_json(client_class: Callable[..., HttpClient]):
    """Without callback=True, the client automatically parses JSON."""
    async with client_class() as client:
        data = await _request_or_skip(client.get("https://httpbin.org/json"))
        assert "slideshow" in data


async def test_client_204_no_content(client_class: Callable[..., HttpClient]):
    """A 204 response returns an empty dict, not an error."""
    async with client_class() as client:
        data = await _request_or_skip(client.delete("https://httpbin.org/status/204"))
        assert data == {}


# ---------------------------------------------------------------------------
# HttpClient – errors
# ---------------------------------------------------------------------------


async def test_client_raises_on_error(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        with pytest.raises(HttpResponseError) as exc_info:
            await _request_or_skip(client.get("https://httpbin.org/status/500"))
        assert exc_info.value.status_code == 500
        assert "request_url" in exc_info.value.data
        assert "response_status" in exc_info.value.data


async def test_client_raises_on_404(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        with pytest.raises(HttpResponseError) as exc_info:
            await _request_or_skip(client.get("https://httpbin.org/status/404"))
        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# HttpClient – custom ok_status
# ---------------------------------------------------------------------------


async def test_client_custom_ok_status(client_class: Callable[..., HttpClient]):
    """Custom ok_status allows non-standard success codes."""
    async with client_class(ok_status=frozenset({200, 201, 202, 204})) as client:
        response = await _request_or_skip(
            client.delete("https://httpbin.org/status/202", callback=True)
        )
        assert response.status_code == 202


# ---------------------------------------------------------------------------
# HttpClient – callback
# ---------------------------------------------------------------------------


async def test_client_custom_callback(client_class: Callable[..., HttpClient]):
    """A custom callback transforms the response."""

    async def transform(r):
        if r.status_code >= 502:
            pytest.skip(f"httpbin.org unavailable ({r.status_code})")
        raw = await r.json()
        return raw.get("url")

    async with client_class() as client:
        url = await _request_or_skip(
            client.get("https://httpbin.org/get", callback=transform)
        )
        assert url == "https://httpbin.org/get"


# ---------------------------------------------------------------------------
# HttpClient – default headers
# ---------------------------------------------------------------------------


async def test_client_sends_user_agent(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        response = await _request_or_skip(
            client.get("https://httpbin.org/headers", callback=True)
        )
        data = await response.json()
        assert "User-Agent" in data["headers"]


async def test_client_custom_headers_merge(client_class: Callable[..., HttpClient]):
    async with client_class(default_headers={"X-Custom": "value"}) as client:
        response = await _request_or_skip(
            client.get("https://httpbin.org/headers", callback=True)
        )
        data = await response.json()
        assert data["headers"]["X-Custom"] == "value"


async def test_client_per_request_headers(client_class: Callable[..., HttpClient]):
    async with client_class() as client:
        response = await _request_or_skip(
            client.get(
                "https://httpbin.org/headers",
                headers={"X-Per-Request": "1"},
                callback=True,
            )
        )
        data = await response.json()
        assert data["headers"]["X-Per-Request"] == "1"


# ---------------------------------------------------------------------------
# HttpClient – no content_type (no accept header)
# ---------------------------------------------------------------------------


async def test_client_no_content_type(client_class: Callable[..., HttpClient]):
    """When content_type is empty, no content-type-specific accept header is set."""
    async with client_class(content_type="") as client:
        response = await _request_or_skip(
            client.get("https://httpbin.org/headers", callback=True)
        )
        data = await response.json()
        # We only assert that application/json is NOT forced as accept
        assert data["headers"].get("Accept", "") != "application/json"
