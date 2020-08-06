from aiohttp.web import Application
from openapi.cli import OpenApiClient

from . import log


def dev_app(cli: OpenApiClient, log_level="DEBUG") -> Application:
    log.setup(log_level)
    return cli.get_serve_app()
