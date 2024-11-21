from __future__ import annotations

from typing import Any, Callable

import uvicorn
from fastapi import FastAPI

from fluid.utils import log
from fluid.utils.worker import Workers

from .service import FastapiAppWorkers


def app_workers(app: FastAPI) -> Workers:
    if workers := getattr(app.state, "workers", None):
        return workers
    else:
        workers = FastapiAppWorkers.setup(app)
        app.state.workers = workers
        return workers


def serve_app(
    app: FastAPI | Callable[..., Any] | str,
    host: str,
    port: int,
    reload: bool = False,
) -> None:
    uvicorn.run(
        app,
        port=port,
        host=host,
        log_level="info",
        reload=reload,
        log_config=log.config(),
    )
