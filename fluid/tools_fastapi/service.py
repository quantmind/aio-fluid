import os
import signal
from typing import Any, Self

from fastapi import FastAPI

from fluid.utils import log
from fluid.utils.worker import Worker, Workers

logger = log.get_logger(__name__)


class FastapiAppWorkers(Workers):
    """An aiohttp runner"""

    @classmethod
    def setup(cls, app: FastAPI, **kwargs: Any) -> Self:
        """Setup the app runner"""
        workers = cls(**kwargs)
        app.state.workers = workers
        app.add_event_handler("startup", workers.startup)
        app.add_event_handler("shutdown", workers.shutdown)
        return workers

    def bail_out(self, reason: str, code: int = 1) -> None:
        logger.warning("shutting down due to %s", reason)
        os.kill(os.getpid(), signal.SIGTERM)

    def get_active_worker(self, *, worker_name: str) -> Worker | None:
        worker = self._workers.get_worker_by_name(worker_name)
        if worker and not worker.stopping:
            return worker
        return None
