import os
import signal
from typing import Any, Self

from fastapi import FastAPI

from fluid import settings
from fluid.utils import log
from fluid.utils.worker import Workers

logger = log.get_logger(__name__)


class FastapiAppWorkers(Workers):

    @classmethod
    def setup(cls, app: FastAPI, **kwargs: Any) -> Self:
        workers = cls(**kwargs)
        app.state.workers = workers
        app.add_event_handler("startup", workers.startup)
        app.add_event_handler("shutdown", workers.shutdown)
        return workers

    def after_shutdown(self, reason: str, code: int = 1) -> None:
        if settings.ENV != "test":
            os.kill(os.getpid(), signal.SIGTERM)
