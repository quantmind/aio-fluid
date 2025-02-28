from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, Request

from fluid.utils.worker import Workers

from .service import FastapiAppWorkers


def get_workers_from_request(request: Request) -> Workers | None:
    """Get workers from request."""
    return get_workers_from_app(request.app)


def get_workers_from_app(app: FastAPI) -> Workers | None:
    """Get workers from request."""
    return getattr(app.state, "workers", None)


def app_workers(app: FastAPI) -> Workers:
    """Get workers from app state or create new workers."""
    if workers := get_workers_from_app(app):
        return workers
    else:
        workers = FastapiAppWorkers.setup(app)
        app.state.workers = workers
        return workers


WorkersDep = Annotated[Workers | None, Depends(get_workers_from_request)]
