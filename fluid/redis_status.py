from typing import Dict, List

from aiohttp import web

routes = web.RouteTableDef()


REDIS_CLIENT = [
    "id",
    "addR",
    "fd",
    "name",
    "age",
    "idle",
    "flags",
    "db",
    "sub",
    "psub",
    "multi",
    "qbuf",
    "qbuf-free",
    "obl",
    "oll",
    "omem",
    "events",
    "cmd",
]


def redis(request):
    return request.app["redis"].pub


@routes.get("/redis/clients")
async def status(request):
    data = await redis(request).client_list()
    return web.json_response([redis_client(d) for d in data])


def redis_client(d: List) -> Dict:
    return dict(zip(REDIS_CLIENT, d))
