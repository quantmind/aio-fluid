import asyncio
import base64
import hashlib
import hmac
import os
from functools import cached_property
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, Union

import aiohttp
from aiohttp import ClientResponse, ClientSession, ClientWebSocketResponse, WSMsgType
from aiohttp.client_exceptions import ContentTypeError
from aiohttp.formdata import FormData
from inflection import underscore

from fluid import json
from fluid.node import NodeWorker
from fluid.types import JsonType, String, to_bytes

from .utils import with_ops


class ResponseError(RuntimeError):
    pass


ObjectResponse = Union[Dict, "Entity"]
ListResponse = List[ObjectResponse]
ResponseType = Union[ListResponse, ObjectResponse, None]


class HttpResponseError(RuntimeError):
    def __init__(self, response: ClientResponse, data: Dict) -> None:
        self.response = response
        self.data = data
        self.data["request_url"] = str(response.url)
        self.data["request_method"] = response.method
        self.data["response_status"] = response.status

    @property
    def status(self) -> int:
        return self.response.status

    def __str__(self) -> str:
        return json.dumps(self.data, indent=4)


class HttpBase:
    @cached_property
    def extra_kwargs(self) -> Tuple[str, ...]:
        return ("wrap", "callback", "headers")

    async def get(self, url: str, **kwargs) -> ResponseType:
        kwargs["method"] = "GET"
        return await self.execute(url, **kwargs)

    async def patch(self, url: str, **kwargs) -> ResponseType:
        kwargs["method"] = "PATCH"
        return await self.execute(url, **kwargs)

    async def post(self, url: str, **kwargs) -> ResponseType:
        kwargs["method"] = "POST"
        return await self.execute(url, **kwargs)

    async def put(self, url: str, **kwargs) -> ResponseType:
        kwargs["method"] = "PUT"
        return await self.execute(url, **kwargs)

    async def delete(self, url: str, **kwargs) -> ResponseType:
        kwargs["method"] = "DELETE"
        return await self.execute(url, **kwargs)

    def nowrap(self, data, response: ClientResponse) -> ObjectResponse:
        return data

    def wrap(self, data, response: ClientResponse) -> ObjectResponse:
        return data

    def wrap_list(self, data, response: ClientResponse) -> ListResponse:
        return [self.wrap(d, response) for d in data]

    @classmethod
    def hmac(
        cls, key: String, message: String, alg: str = None, digest: str = "hex"
    ) -> String:
        sig = hmac.new(to_bytes(key), to_bytes(message), alg or "sha256")
        if digest == "hex":
            return sig.hexdigest()
        elif digest == "base64":
            return base64.b64encode(sig.digest())
        else:
            return sig.digest()

    @classmethod
    def hash(cls, data: String, alg: str = None, digest: str = "hex") -> String:
        h = hashlib.new(alg or "sha256", to_bytes(data))
        if digest == "hex":
            return h.hexdigest()
        elif digest == "base64":
            return base64.b64encode(h.digest())
        return h.digest()

    def kwargs(self, kwargs: Dict, key_name: str = None, **kw) -> Dict:
        """Expand a key arguments dictionary by extracting additional key valued
        parameters
        """
        if key_name and key_name not in kwargs:
            data = with_ops(kwargs, key_name)
            result = {key_name: data}
            result.update(
                ((key, data.pop(key)) for key in self.extra_kwargs if key in data)
            )
        else:
            result = kwargs.copy()
        result.update(kw)
        return result

    async def form_data(self, data: Dict) -> Coroutine[Any, Any, bytes]:
        return await FD(data).data()


class HttpClient(HttpBase):
    """Base class for Http clients"""

    session: ClientSession = None
    session_owner: bool = False
    user_agent: str = os.getenv("HTTP_USER_AGENT", "quantmind")
    content_type: str = "application/json"
    ResponseError: HttpResponseError = HttpResponseError
    ok_status = frozenset((200, 201))

    @property
    def cli(self) -> "HttpClient":
        return self

    def get_session(self) -> ClientSession:
        if not self.session:
            self.session_owner = True
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self) -> None:
        if self.session and self.session_owner:
            await self.session.close()
            self.session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def execute(
        self,
        url: str,
        *,
        method: str = None,
        headers: Dict = None,
        callback=None,
        wrap=None,
        **kw,
    ) -> ResponseType:
        session = self.get_session()
        _headers = self.default_headers()
        _headers.update(headers or ())
        method = method or "GET"
        response = await session.request(method, url, headers=_headers, **kw)
        if callback:
            return await callback(response)
        if response.status in self.ok_status:
            data = await self.response_data(response)
        elif response.status == 204:
            return
        else:
            data = await self.response_error(response)
        wrap = wrap or self.wrap
        return wrap(data, response)

    def default_headers(self) -> Dict[str, str]:
        return {"user-agent": self.user_agent, "accept": self.content_type}

    def mock(self) -> None:
        pass

    @classmethod
    async def response_error(cls, response: ClientResponse) -> ResponseType:
        try:
            data = await cls.response_data(response)
        except ContentTypeError:
            data = dict(message=await response.text())
        raise cls.ResponseError(response, data)

    @classmethod
    async def response_data(cls, response):
        return await response.json()


class HttpComponent(HttpBase):
    @property
    def cli(self) -> HttpClient:
        return self.root.cli

    def get_session(self) -> ClientSession:
        return self.cli.get_session()

    async def execute(self, url, **params):
        params.setdefault("wrap", self.wrap)
        return await self.cli.execute(url, **params)


class Component(HttpComponent):
    def __init__(self, root: HttpClient, name: str = "") -> None:
        self.root = root
        self.path = name or underscore(type(self).__name__)

    def __repr__(self) -> str:
        return self.url

    def __str__(self) -> str:
        return self.url

    @cached_property
    def url(self) -> str:
        return f"{self.cli.url}/{self.path}"


class WsConnection(NodeWorker):
    """Worker for listening to stream of websocket messages"""

    def __init__(
        self,
        ws_component: "WsComponent",
        ws_url: str,
        ws_connection: ClientWebSocketResponse,
    ):
        super().__init__()
        self.ws_component = ws_component
        self.ws_url = ws_url
        self.ws_connection = ws_connection
        self.on_text_message = None
        self.on_binary_message = None
        ws_component.ws_connections[ws_url] = self

    async def write_json(self, data: JsonType) -> None:
        await self.write_str(json.dumps(data))

    async def write_str(self, msg: str) -> None:
        await self.ws_connection.send_str(msg)

    async def teardown(self) -> None:
        self.ws_component.ws_connections.pop(self.ws_url, None)
        if self.ws_component.on_disconnect:
            self.ws_component.on_disconnect(self)

    async def work(self) -> None:
        async for msg in self.ws_connection:
            if msg.type == WSMsgType.TEXT:
                if self.on_text_message:
                    try:
                        self.on_text_message(msg.data)
                    except Exception:
                        self.logger.exception(
                            "Critical failure while broadcasting text message"
                        )
            elif msg.type == WSMsgType.BINARY:
                if self.on_binary_message:
                    try:
                        self.on_binary_message(msg.data)
                    except Exception:
                        self.logger.exception(
                            "Critical failure while broadcasting binary message"
                        )
            # release the loop for greedy websocket connections
            await asyncio.sleep(0)


class WsComponent(Component):
    connection_factory = WsConnection
    on_disconnect: Optional[Callable[[WsConnection], None]] = None
    heartbeat: Optional[float] = None

    @cached_property
    def ws_connections(self) -> Dict[str, WsConnection]:
        return {}

    async def connect_and_listen(
        self,
        ws_url: str,
        on_text_message: Callable[[str], None] = None,
        on_binary_message: Callable[[bytes], None] = None,
        **kw,
    ) -> WsConnection:
        connection = self.ws_connections.get(ws_url)
        if not connection:
            if self.heartbeat:
                kw.setdefault("heartbeat", self.heartbeat)
            ws = await self.get_session().ws_connect(ws_url, **kw)
            connection = self.connection_factory(self, ws_url, ws)
            await connection.start()
        if on_text_message:
            connection.on_text_message = on_text_message
        if on_binary_message:
            connection.on_binary_message = on_binary_message
        return connection

    async def close(self) -> None:
        if self.ws_connections:
            ws = self.ws_connections.copy()
            self.ws_connections.clear()
            await asyncio.gather(*[c.close() for c in ws.values()])

    def wrap_json(self, on_message) -> Callable[[JsonType], None]:
        def on_text_message(msg: str) -> None:
            on_message(json.loads(msg))

        return on_text_message if on_message else on_message


class CrudMixin:
    async def get_list(self, **params) -> ListResponse:
        params.setdefault("wrap", self.wrap_list)
        return await self.execute(self.url, **self.kwargs(params, "params"))

    async def create_one(self, **kwargs) -> ObjectResponse:
        return await self.post(self.url, **self.kwargs(kwargs, "json"))

    create = create_one

    async def get_one(self, id_, **kwargs) -> ObjectResponse:
        return await self.get(f"{self.url}/{id_}", **self.kwargs(kwargs))

    async def update_one(self, id_, **kwargs) -> ObjectResponse:
        return await self.patch(f"{self.url}/{id_}", **self.kwargs(kwargs, "json"))

    async def delete_one(self, id_) -> None:
        return await self.delete(f"{self.url}/{id_}")


class Entity(HttpComponent):
    __slots__ = ("root", "data")

    def __init__(self, root: Component, data: Dict) -> None:
        self.root = root
        self.data = data

    def __repr__(self) -> str:
        return self.data.__repr__()

    def __str__(self) -> str:
        return self.data.__str__()

    def __getitem__(self, key: str) -> Any:
        return self.data[key]


class CRUDEntity(Entity):
    @property
    def id(self) -> str:
        return self.data.get("id")

    @property
    def url(self) -> str:
        return "%s/%s" % (self.root.url, self.id)


class FD:
    def __init__(self, data: Dict) -> None:
        fd = FormData(data)
        self.payload = fd()
        self.value = None

    async def data(self):
        await self.payload.write(self)
        return self.value

    async def write(self, value: bytes) -> None:
        self.value = value
