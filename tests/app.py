from aiohttp import web
from openapi import ws
from openapi.rest import rest
from openapi.spec.path import ApiPath
from openapi.ws import pubsub
from openapi.ws.manager import SocketsManager

from fluid import service
from fluid.http import HttpClient, WsComponent

ws_routes = web.RouteTableDef()


def create_app():
    return rest(
        setup_app=setup_app,
    )


def setup_app(app: web.Application) -> None:
    service.setup(
        app,
        sampler=True,
    )
    app["web_sockets"] = SocketsManager()
    app.router.add_routes(ws_routes)


@ws_routes.view("/stream")
class StreamPath(ws.WsPathMixin, pubsub.Publish, pubsub.Subscribe, ApiPath):
    """
    ---
    summary: Create and query Tasks
    tags:
        - Task
    """

    async def ws_rpc_echo(self, payload):
        """Echo parameters"""
        return payload


class AppClient(HttpClient):
    def __init__(self, url: str) -> None:
        self.url = url
        self.stream = WsComponent(self)

    async def close(self) -> None:
        await self.stream.close()
        return await super().close()
