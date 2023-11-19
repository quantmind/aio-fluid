import logging
from dataclasses import dataclass

from fluid.db import Database

logger = logging.getLogger(__name__)


@dataclass
class AioCtx:
    db: Database

    async def __call__(self):
        yield
        logger.warning("closing database connection %s", repr(self.db.engine.url))
        await self.db.close()
