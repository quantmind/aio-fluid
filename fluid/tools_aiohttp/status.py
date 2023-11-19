import os

from aiohttp import web
from openapi.spec.server import server_urls
from openapi.tz import utcnow

__version__ = os.environ.get("GIT_SHA", "unknown")
__timestamp__ = os.environ.get("TIMESTAMP", utcnow().isoformat())


status_routes = web.RouteTableDef()


@status_routes.get("/status")
async def status(request):
    service_status = request.app.get("service_status")
    if service_status:
        await service_status(request)
    result = dict(ok=True, sha=__version__, timestamp=__timestamp__)
    return web.json_response(result)


@status_routes.get("/headers")
async def headers(request):
    result = dict(request.headers.items())
    return web.json_response(result)


@status_routes.get("/")
async def urls(request):
    paths = set()
    for route in request.app.router.routes():
        route_info = route.get_info()
        path = route_info.get("path", route_info.get("formatter", None))
        paths.add(path)
    return web.json_response(server_urls(request, sorted(paths)))
