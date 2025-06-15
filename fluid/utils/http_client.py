from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from inspect import Traceback
from typing import Any, Callable, Generic, Self, Type, TypeVar

import httpx
from aiohttp import client

from fluid import settings

from .http_monitor import HttpPathFn, monitor_http_call


class ResponseError(RuntimeError):
    pass


ResponseType = Any
S = TypeVar("S", bound=client.ClientSession | httpx.AsyncClient)
R = TypeVar("R", bound=client.ClientResponse | httpx.Response)


class HttpResponse(ABC):
    @property
    @abstractmethod
    def url(self) -> str: ...

    @property
    @abstractmethod
    def status_code(self) -> int: ...

    @property
    @abstractmethod
    def method(self) -> str: ...

    @property
    @abstractmethod
    def headers(self) -> dict[str, str]: ...

    @abstractmethod
    async def json(self) -> ResponseType: ...

    @abstractmethod
    async def text(self) -> str: ...


class HttpResponseError(RuntimeError):
    def __init__(self, response: HttpResponse, data: ResponseType) -> None:
        self.response = response
        self.data = {
            "response": data,
            "request_url": response.url,
            "request_method": response.method,
            "response_status": self.status_code,
        }

    @property
    def status_code(self) -> int:
        return self.response.status_code

    def __str__(self) -> str:
        return json.dumps(self.data, indent=2)


@dataclass
class GenericHttpResponse(HttpResponse, Generic[R]):
    response: R

    @property
    def headers(self) -> dict[str, str]:
        return self.response.headers  # type: ignore


@dataclass
class AioHttpResponse(GenericHttpResponse[client.ClientResponse]):
    @property
    def url(self) -> str:
        return str(self.response.url)

    @property
    def status_code(self) -> int:
        return self.response.status

    @property
    def method(self) -> str:
        return self.response.method

    async def json(self) -> ResponseType:
        return await self.response.json()

    async def text(self) -> str:
        return await self.response.text()


@dataclass
class HttpxResponse(GenericHttpResponse[httpx.Response]):
    @property
    def url(self) -> str:
        return str(self.response.url)

    @property
    def status_code(self) -> int:
        return self.response.status_code

    @property
    def method(self) -> str:
        return self.response.request.method

    async def json(self) -> ResponseType:
        return self.response.json()

    async def text(self) -> str:
        return self.response.text


@dataclass
class HttpClient(Generic[S, R], ABC):
    """Base class for Http clients"""

    session: S | None = None
    content_type: str = "application/json"
    session_owner: bool = False
    ResponseError: Type[HttpResponseError] = field(
        default=HttpResponseError, repr=False
    )
    ok_status: frozenset = field(default=frozenset((200, 201)), repr=False)
    default_headers: dict[str, str] = field(
        default_factory=lambda: {
            "user-agent": settings.HTTP_USER_AGENT,
        }
    )

    @abstractmethod
    def new_session(self, **kwargs: Any) -> S: ...

    @abstractmethod
    def new_response(self, response: R) -> GenericHttpResponse[R]: ...

    @abstractmethod
    async def close(self) -> None: ...

    def get_session(self) -> S:
        if not self.session:
            self.session_owner = True
            self.session = self.new_session()
        return self.session

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: Type,
        exc: Type[BaseException],
        tb: Traceback,
    ) -> None:
        await self.close()

    async def get(self, url: str, **kwargs: Any) -> ResponseType:
        return await self.request("GET", url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> ResponseType:
        return await self.request("PATCH", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> ResponseType:
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> ResponseType:
        return await self.request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> ResponseType:
        return await self.request("DELETE", url, **kwargs)

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

    def ok(self, response: HttpResponse) -> bool:
        return response.status_code in self.ok_status

    def get_default_headers(self) -> dict[str, str]:
        headers = self.default_headers.copy()
        if self.content_type:
            headers["accept"] = self.content_type
        return headers

    @classmethod
    async def response_error(cls, response: HttpResponse) -> None:
        try:
            data = await cls.response_data(response)
        except Exception:
            data = {"message": await response.text()}
        raise cls.ResponseError(response, data)

    @classmethod
    async def response_data(cls, response: HttpResponse) -> ResponseType:
        if "text/csv" in response.headers["content-type"]:
            return await response.text()
        return await response.json()


class AioHttpClient(HttpClient[client.ClientSession, client.ClientResponse]):
    def new_session(self, **kwargs: Any) -> client.ClientSession:
        return client.ClientSession(**kwargs)

    def new_response(
        self, response: client.ClientResponse
    ) -> GenericHttpResponse[client.ClientResponse]:
        return AioHttpResponse(response)

    async def close(self) -> None:
        if self.session and self.session_owner:
            await self.session.close()
            self.session = None


class HttpxClient(HttpClient[httpx.AsyncClient, httpx.Response]):
    def new_session(self, **kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(**kwargs)

    def new_response(
        self, response: httpx.Response
    ) -> GenericHttpResponse[httpx.Response]:
        return HttpxResponse(response)

    async def close(self) -> None:
        if self.session and self.session_owner:
            await self.session.aclose()
            self.session = None
