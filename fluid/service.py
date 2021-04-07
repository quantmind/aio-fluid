import logging
import os
from typing import Callable, Optional

from aiohttp.web import Application
from openapi.middleware import json_error, sentry_middleware
from prometheus_async import aio

from . import backdoor, stacksampler, status

logger = logging.getLogger("fluid.service")


class Service:
    def __init__(self, env: str, metrics_path: str = "") -> None:
        self.env = env
        self.sampler = stacksampler.Sampler()

    async def on_startup(self, app: Application) -> None:
        pass

    async def on_shutdown(self, app: Application) -> None:
        pass

    @classmethod
    def setup(
        cls,
        app: Application = None,
        sentry_dsn: str = "",
        backdoor_port: int = 0,
        service_status: Optional[Callable] = None,
        status_routes: bool = True,
        metrics_path: str = "/metrics",
        sampler: bool = False,
    ) -> Application:
        """Setup boilerplate for asyncio services"""
        if app is None:
            app = Application()
        # create service object
        service = cls(app.get("env") or os.getenv("PYTHON_ENV", "prod"))
        #
        # Add metrics
        if metrics_path:
            app.router.add_get(metrics_path, aio.web.server_stats)
        #
        app["service"] = service
        if service_status:
            app["service_status"] = service_status
        app.on_startup.append(service.on_startup)
        app.on_shutdown.insert(0, service.on_shutdown)
        # add app health probes
        if status_routes:
            app.router.add_routes(status.status_routes)
        # add error handlers
        app.middlewares.append(json_error(status_codes=set(range(400, 504))))
        # add sentry handler
        if sentry_dsn:
            sentry_middleware(app, sentry_dsn, service.env)
        # add backdoor
        if backdoor_port:
            backdoor.setup(app, backdoor_port)

        if sampler:
            app.router.add_routes(stacksampler.sampler_routes)
        return app


setup = Service.setup
