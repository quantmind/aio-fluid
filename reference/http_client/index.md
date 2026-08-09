# HTTP Client

`aio-fluid` provides async HTTP client wrappers around [aiohttp](https://docs.aiohttp.org/) and [httpx](https://www.python-httpx.org/) with a unified interface for making requests, handling errors, and monitoring calls.

```python
from fluid.utils.http_client import AioHttpClient, HttpxClient
```

Both clients implement the same `HttpClient` base class, so they can be used interchangeably.

## Usage

```python
async with AioHttpClient() as client:
    data = await client.get("https://api.example.com/items")

async with HttpxClient() as client:
    data = await client.post("https://api.example.com/items", json={"name": "foo"})
```

## Response

## fluid.utils.http_client.HttpResponse

Bases: `ABC`

### url

```python
url
```

### status_code

```python
status_code
```

### method

```python
method
```

### headers

```python
headers
```

### json

```python
json()
```

Source code in `fluid/utils/http_client.py`

```python
@abstractmethod
async def json(self) -> ResponseType: ...
```

### text

```python
text()
```

Source code in `fluid/utils/http_client.py`

```python
@abstractmethod
async def text(self) -> str: ...
```

### bytes

```python
bytes()
```

Source code in `fluid/utils/http_client.py`

```python
@abstractmethod
async def bytes(self) -> bytes: ...
```

## fluid.utils.http_client.HttpResponseError

```python
HttpResponseError(response, data)
```

Bases: `RuntimeError`

Source code in `fluid/utils/http_client.py`

```python
def __init__(self, response: HttpResponse, data: ResponseType) -> None:
    self.response = response
    self.data = {
        "response": data,
        "request_url": response.url,
        "request_method": response.method,
        "response_status": self.status_code,
    }
```

### response

```python
response = response
```

### data

```python
data = {
    "response": data,
    "request_url": response.url,
    "request_method": response.method,
    "response_status": self.status_code,
}
```

### status_code

```python
status_code
```

## Clients

## fluid.utils.http_client.HttpClient

```python
HttpClient(
    session=None,
    content_type="application/json",
    session_owner=False,
    ResponseError=HttpResponseError,
    ok_status=frozenset((200, 201)),
    default_headers=(
        lambda: {"user-agent": HTTP_USER_AGENT}
    )(),
)
```

Bases: `Generic[S, R]`, `ABC`

Base class for Http clients

### session

```python
session = None
```

### content_type

```python
content_type = 'application/json'
```

### session_owner

```python
session_owner = False
```

### ResponseError

```python
ResponseError = field(default=HttpResponseError, repr=False)
```

### ok_status

```python
ok_status = field(default=frozenset((200, 201)), repr=False)
```

### default_headers

```python
default_headers = field(
    default_factory=lambda: {
        "user-agent": settings.HTTP_USER_AGENT
    }
)
```

### new_session

```python
new_session(**kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
@abstractmethod
def new_session(self, **kwargs: Any) -> S: ...
```

### new_response

```python
new_response(response)
```

Source code in `fluid/utils/http_client.py`

```python
@abstractmethod
def new_response(self, response: R) -> GenericHttpResponse[R]: ...
```

### close

```python
close()
```

Source code in `fluid/utils/http_client.py`

```python
@abstractmethod
async def close(self) -> None: ...
```

### get_session

```python
get_session()
```

Source code in `fluid/utils/http_client.py`

```python
def get_session(self) -> S:
    if not self.session:
        self.session_owner = True
        self.session = self.new_session()
    return self.session
```

### get

```python
get(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def get(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("GET", url, **kwargs)
```

### patch

```python
patch(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def patch(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("PATCH", url, **kwargs)
```

### post

```python
post(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def post(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("POST", url, **kwargs)
```

### put

```python
put(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def put(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("PUT", url, **kwargs)
```

### delete

```python
delete(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def delete(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("DELETE", url, **kwargs)
```

### request

```python
request(
    method,
    url,
    *,
    headers=None,
    callback=None,
    monitor_http=None,
    **kw
)
```

Source code in `fluid/utils/http_client.py`

```python
async def request(
    self,
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    callback: Callable | bool | None = None,
    monitor_http: HttpPathFn | None = None,
    **kw: Any,
) -> ResponseType:
    session = self.get_session()
    _headers = self.get_default_headers()
    _headers.update(headers or ())
    method = method or "GET"
    start = time.monotonic()
    inner: R = await session.request(
        method,
        url,
        headers=_headers,
        **kw,
    )  # type: ignore
    response = self.new_response(inner)
    if monitor_http:
        monitor_http_call(
            response,
            time.monotonic() - start,
            sanitization_fn=monitor_http,
        )
    if callback:
        if callback is True:
            return response
        else:
            return await callback(response)
    if self.ok(response):
        data = await self.response_data(response)
    elif response.status_code == 204:
        data = {}
    else:
        await self.response_error(response)
    return data
```

### ok

```python
ok(response)
```

Source code in `fluid/utils/http_client.py`

```python
def ok(self, response: HttpResponse) -> bool:
    return response.status_code in self.ok_status
```

### get_default_headers

```python
get_default_headers()
```

Source code in `fluid/utils/http_client.py`

```python
def get_default_headers(self) -> dict[str, str]:
    headers = self.default_headers.copy()
    if self.content_type:
        headers["accept"] = self.content_type
    return headers
```

### response_error

```python
response_error(response)
```

Source code in `fluid/utils/http_client.py`

```python
@classmethod
async def response_error(cls, response: HttpResponse) -> None:
    try:
        data = await cls.response_data(response)
    except Exception:
        data = {"message": await response.text()}
    raise cls.ResponseError(response, data)
```

### response_data

```python
response_data(response)
```

Source code in `fluid/utils/http_client.py`

```python
@classmethod
async def response_data(cls, response: HttpResponse) -> ResponseType:
    content_type = response.headers.get("content-type", "")
    if "json" in content_type:
        return await response.json()
    elif "text" in content_type:
        return await response.text()
    return await response.bytes()
```

## fluid.utils.http_client.AioHttpClient

```python
AioHttpClient(
    session=None,
    content_type="application/json",
    session_owner=False,
    ResponseError=HttpResponseError,
    ok_status=frozenset((200, 201)),
    default_headers=(
        lambda: {"user-agent": HTTP_USER_AGENT}
    )(),
)
```

Bases: `HttpClient[ClientSession, ClientResponse]`

### session

```python
session = None
```

### content_type

```python
content_type = 'application/json'
```

### session_owner

```python
session_owner = False
```

### ResponseError

```python
ResponseError = field(default=HttpResponseError, repr=False)
```

### ok_status

```python
ok_status = field(default=frozenset((200, 201)), repr=False)
```

### default_headers

```python
default_headers = field(
    default_factory=lambda: {
        "user-agent": settings.HTTP_USER_AGENT
    }
)
```

### new_session

```python
new_session(**kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
def new_session(self, **kwargs: Any) -> client.ClientSession:
    return client.ClientSession(**kwargs)
```

### new_response

```python
new_response(response)
```

Source code in `fluid/utils/http_client.py`

```python
def new_response(
    self, response: client.ClientResponse
) -> GenericHttpResponse[client.ClientResponse]:
    return AioHttpResponse(response)
```

### close

```python
close()
```

Source code in `fluid/utils/http_client.py`

```python
async def close(self) -> None:
    if self.session and self.session_owner:
        await self.session.close()
        self.session = None
```

### get_session

```python
get_session()
```

Source code in `fluid/utils/http_client.py`

```python
def get_session(self) -> S:
    if not self.session:
        self.session_owner = True
        self.session = self.new_session()
    return self.session
```

### get

```python
get(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def get(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("GET", url, **kwargs)
```

### patch

```python
patch(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def patch(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("PATCH", url, **kwargs)
```

### post

```python
post(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def post(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("POST", url, **kwargs)
```

### put

```python
put(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def put(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("PUT", url, **kwargs)
```

### delete

```python
delete(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def delete(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("DELETE", url, **kwargs)
```

### request

```python
request(
    method,
    url,
    *,
    headers=None,
    callback=None,
    monitor_http=None,
    **kw
)
```

Source code in `fluid/utils/http_client.py`

```python
async def request(
    self,
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    callback: Callable | bool | None = None,
    monitor_http: HttpPathFn | None = None,
    **kw: Any,
) -> ResponseType:
    session = self.get_session()
    _headers = self.get_default_headers()
    _headers.update(headers or ())
    method = method or "GET"
    start = time.monotonic()
    inner: R = await session.request(
        method,
        url,
        headers=_headers,
        **kw,
    )  # type: ignore
    response = self.new_response(inner)
    if monitor_http:
        monitor_http_call(
            response,
            time.monotonic() - start,
            sanitization_fn=monitor_http,
        )
    if callback:
        if callback is True:
            return response
        else:
            return await callback(response)
    if self.ok(response):
        data = await self.response_data(response)
    elif response.status_code == 204:
        data = {}
    else:
        await self.response_error(response)
    return data
```

### ok

```python
ok(response)
```

Source code in `fluid/utils/http_client.py`

```python
def ok(self, response: HttpResponse) -> bool:
    return response.status_code in self.ok_status
```

### get_default_headers

```python
get_default_headers()
```

Source code in `fluid/utils/http_client.py`

```python
def get_default_headers(self) -> dict[str, str]:
    headers = self.default_headers.copy()
    if self.content_type:
        headers["accept"] = self.content_type
    return headers
```

### response_error

```python
response_error(response)
```

Source code in `fluid/utils/http_client.py`

```python
@classmethod
async def response_error(cls, response: HttpResponse) -> None:
    try:
        data = await cls.response_data(response)
    except Exception:
        data = {"message": await response.text()}
    raise cls.ResponseError(response, data)
```

### response_data

```python
response_data(response)
```

Source code in `fluid/utils/http_client.py`

```python
@classmethod
async def response_data(cls, response: HttpResponse) -> ResponseType:
    content_type = response.headers.get("content-type", "")
    if "json" in content_type:
        return await response.json()
    elif "text" in content_type:
        return await response.text()
    return await response.bytes()
```

## fluid.utils.http_client.HttpxClient

```python
HttpxClient(
    session=None,
    content_type="application/json",
    session_owner=False,
    ResponseError=HttpResponseError,
    ok_status=frozenset((200, 201)),
    default_headers=(
        lambda: {"user-agent": HTTP_USER_AGENT}
    )(),
)
```

Bases: `HttpClient[AsyncClient, Response]`

### session

```python
session = None
```

### content_type

```python
content_type = 'application/json'
```

### session_owner

```python
session_owner = False
```

### ResponseError

```python
ResponseError = field(default=HttpResponseError, repr=False)
```

### ok_status

```python
ok_status = field(default=frozenset((200, 201)), repr=False)
```

### default_headers

```python
default_headers = field(
    default_factory=lambda: {
        "user-agent": settings.HTTP_USER_AGENT
    }
)
```

### new_session

```python
new_session(**kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
def new_session(self, **kwargs: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(**kwargs)
```

### new_response

```python
new_response(response)
```

Source code in `fluid/utils/http_client.py`

```python
def new_response(
    self, response: httpx.Response
) -> GenericHttpResponse[httpx.Response]:
    return HttpxResponse(response)
```

### close

```python
close()
```

Source code in `fluid/utils/http_client.py`

```python
async def close(self) -> None:
    if self.session and self.session_owner:
        await self.session.aclose()
        self.session = None
```

### get_session

```python
get_session()
```

Source code in `fluid/utils/http_client.py`

```python
def get_session(self) -> S:
    if not self.session:
        self.session_owner = True
        self.session = self.new_session()
    return self.session
```

### get

```python
get(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def get(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("GET", url, **kwargs)
```

### patch

```python
patch(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def patch(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("PATCH", url, **kwargs)
```

### post

```python
post(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def post(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("POST", url, **kwargs)
```

### put

```python
put(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def put(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("PUT", url, **kwargs)
```

### delete

```python
delete(url, **kwargs)
```

Source code in `fluid/utils/http_client.py`

```python
async def delete(self, url: str, **kwargs: Any) -> ResponseType:
    return await self.request("DELETE", url, **kwargs)
```

### request

```python
request(
    method,
    url,
    *,
    headers=None,
    callback=None,
    monitor_http=None,
    **kw
)
```

Source code in `fluid/utils/http_client.py`

```python
async def request(
    self,
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    callback: Callable | bool | None = None,
    monitor_http: HttpPathFn | None = None,
    **kw: Any,
) -> ResponseType:
    session = self.get_session()
    _headers = self.get_default_headers()
    _headers.update(headers or ())
    method = method or "GET"
    start = time.monotonic()
    inner: R = await session.request(
        method,
        url,
        headers=_headers,
        **kw,
    )  # type: ignore
    response = self.new_response(inner)
    if monitor_http:
        monitor_http_call(
            response,
            time.monotonic() - start,
            sanitization_fn=monitor_http,
        )
    if callback:
        if callback is True:
            return response
        else:
            return await callback(response)
    if self.ok(response):
        data = await self.response_data(response)
    elif response.status_code == 204:
        data = {}
    else:
        await self.response_error(response)
    return data
```

### ok

```python
ok(response)
```

Source code in `fluid/utils/http_client.py`

```python
def ok(self, response: HttpResponse) -> bool:
    return response.status_code in self.ok_status
```

### get_default_headers

```python
get_default_headers()
```

Source code in `fluid/utils/http_client.py`

```python
def get_default_headers(self) -> dict[str, str]:
    headers = self.default_headers.copy()
    if self.content_type:
        headers["accept"] = self.content_type
    return headers
```

### response_error

```python
response_error(response)
```

Source code in `fluid/utils/http_client.py`

```python
@classmethod
async def response_error(cls, response: HttpResponse) -> None:
    try:
        data = await cls.response_data(response)
    except Exception:
        data = {"message": await response.text()}
    raise cls.ResponseError(response, data)
```

### response_data

```python
response_data(response)
```

Source code in `fluid/utils/http_client.py`

```python
@classmethod
async def response_data(cls, response: HttpResponse) -> ResponseType:
    content_type = response.headers.get("content-type", "")
    if "json" in content_type:
        return await response.json()
    elif "text" in content_type:
        return await response.text()
    return await response.bytes()
```
