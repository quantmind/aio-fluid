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

::: fluid.utils.http_client.HttpResponse

::: fluid.utils.http_client.HttpResponseError

## Clients

::: fluid.utils.http_client.HttpClient

::: fluid.utils.http_client.AioHttpClient

::: fluid.utils.http_client.HttpxClient
