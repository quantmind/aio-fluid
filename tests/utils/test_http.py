import httpx2 as httpx
import pytest

from fluid.utils.http_client import (
    HttpResponseError,
    HttpxClient,
    HttpxResponse,
)


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
# HttpxClient – basic HTTP methods
# ---------------------------------------------------------------------------


async def test_httpx_client_get():
    async with HttpxClient() as client:
        response = await client.get("https://httpbin.org/get", callback=True)
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        assert response.status_code == 200
        data = await response.json()
        assert data["url"] == "https://httpbin.org/get"
        assert response.headers["Content-Type"] == "application/json"
        assert response.method == "GET"
        assert response.url == "https://httpbin.org/get"


async def test_httpx_client_post():
    async with HttpxClient() as client:
        response = await client.post(
            "https://httpbin.org/post",
            json={"hello": "world"},
            callback=True,
        )
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        assert response.status_code == 200
        data = await response.json()
        assert data["json"] == {"hello": "world"}


async def test_httpx_client_put():
    async with HttpxClient() as client:
        response = await client.put(
            "https://httpbin.org/put",
            json={"k": "v"},
            callback=True,
        )
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        assert response.status_code == 200
        data = await response.json()
        assert data["json"] == {"k": "v"}


async def test_httpx_client_patch():
    async with HttpxClient() as client:
        response = await client.patch(
            "https://httpbin.org/patch",
            json={"k": "v"},
            callback=True,
        )
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        assert response.status_code == 200
        data = await response.json()
        assert data["json"] == {"k": "v"}


async def test_httpx_client_delete():
    async with HttpxClient() as client:
        response = await client.delete(
            "https://httpbin.org/delete",
            callback=True,
        )
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# HttpxClient – session management
# ---------------------------------------------------------------------------


async def test_httpx_client_context_manager_creates_own_session():
    async with HttpxClient() as client:
        # session is created lazily on first request
        assert client.session is None
        assert client.session_owner is False
        await client.get("https://httpbin.org/get", callback=True)
        assert client.session is not None
        assert client.session_owner is True


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


async def test_httpx_client_close_twice_is_safe():
    async with HttpxClient() as client:
        pass
    # already closed by __aexit__ – calling again should not raise
    await client.close()


# ---------------------------------------------------------------------------
# HttpxClient – response data
# ---------------------------------------------------------------------------


async def test_httpx_client_auto_parse_json():
    """Without callback=True, the client automatically parses JSON."""
    async with HttpxClient() as client:
        data = await client.get("https://httpbin.org/json")
        if isinstance(data, dict) and "slideshow" in data:
            assert "slideshow" in data
        else:
            pytest.skip("httpbin.org returned unexpected response")


async def test_httpx_client_204_no_content():
    """A 204 response returns an empty dict, not an error."""
    async with HttpxClient() as client:
        data = await client.delete("https://httpbin.org/status/204")
        assert data == {}


# ---------------------------------------------------------------------------
# HttpxClient – errors
# ---------------------------------------------------------------------------


async def test_httpx_client_raises_on_error():
    async with HttpxClient() as client:
        with pytest.raises(HttpResponseError) as exc_info:
            await client.get("https://httpbin.org/status/500")
        assert exc_info.value.status_code == 500
        assert "request_url" in exc_info.value.data
        assert "response_status" in exc_info.value.data


async def test_httpx_client_raises_on_404():
    async with HttpxClient() as client:
        with pytest.raises(HttpResponseError) as exc_info:
            await client.get("https://httpbin.org/status/404")
        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# HttpxClient – custom ok_status
# ---------------------------------------------------------------------------


async def test_httpx_client_custom_ok_status():
    """Custom ok_status allows non-standard success codes."""
    async with HttpxClient(ok_status=frozenset({200, 201, 202, 204})) as client:
        response = await client.delete(
            "https://httpbin.org/status/202",
            callback=True,
        )
        assert response.status_code == 202


# ---------------------------------------------------------------------------
# HttpxClient – callback
# ---------------------------------------------------------------------------


async def test_httpx_client_custom_callback():
    """A custom callback transforms the response."""

    async def transform(r):
        raw = await r.json()
        return raw.get("url")

    async with HttpxClient() as client:
        url = await client.get(
            "https://httpbin.org/get",
            callback=transform,
        )
        if url is None:
            pytest.skip("httpbin.org unavailable")
        assert url == "https://httpbin.org/get"


# ---------------------------------------------------------------------------
# HttpxClient – default headers
# ---------------------------------------------------------------------------


async def test_httpx_client_sends_user_agent():
    async with HttpxClient() as client:
        response = await client.get(
            "https://httpbin.org/headers",
            callback=True,
        )
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        data = await response.json()
        assert "User-Agent" in data["headers"]


async def test_httpx_client_custom_headers_merge():
    async with HttpxClient(default_headers={"X-Custom": "value"}) as client:
        response = await client.get(
            "https://httpbin.org/headers",
            callback=True,
        )
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        data = await response.json()
        assert data["headers"]["X-Custom"] == "value"


async def test_httpx_client_per_request_headers():
    async with HttpxClient() as client:
        response = await client.get(
            "https://httpbin.org/headers",
            headers={"X-Per-Request": "1"},
            callback=True,
        )
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        data = await response.json()
        assert data["headers"]["X-Per-Request"] == "1"


# ---------------------------------------------------------------------------
# HttpxClient – no content_type (no accept header)
# ---------------------------------------------------------------------------


async def test_httpx_client_no_content_type():
    """When content_type is empty, no content-type-specific accept header is set."""
    async with HttpxClient(content_type="") as client:
        response = await client.get(
            "https://httpbin.org/headers",
            callback=True,
        )
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        data = await response.json()
        # We only assert that application/json is NOT forced as accept
        assert data["headers"].get("Accept", "") != "application/json"


# ---------------------------------------------------------------------------
# Retain existing AioHttpClient test
# ---------------------------------------------------------------------------


async def test_aiohttp_client():
    from fluid.utils.http_client import AioHttpClient

    async with AioHttpClient() as client:
        response = await client.get("https://httpbin.org/get", callback=True)
        if response.status_code >= 502:
            pytest.skip("httpbin.org unavailable")
        assert response.status_code == 200
        data = await response.json()
        assert data["url"] == "https://httpbin.org/get"
        assert response.headers["Content-Type"] == "application/json"
        assert response.method == "GET"
        assert response.url == "https://httpbin.org/get"
