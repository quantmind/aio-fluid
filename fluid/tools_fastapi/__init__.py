from __future__ import annotations

from fastapi import FastAPI

from fluid.utils.worker import Workers

from .service import FastapiAppWorkers


def app_workers(app: FastAPI) -> Workers:
    """Get workers from app state or create new workers."""
    if workers := getattr(app.state, "workers", None):
        return workers
    else:
        workers = FastapiAppWorkers.setup(app)
        app.state.workers = workers
        return workers
