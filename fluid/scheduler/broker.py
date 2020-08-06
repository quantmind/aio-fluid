import os
from abc import ABC, abstractmethod
from typing import Callable, Dict
from urllib.parse import urlparse

_brokers = {}


class Broker(ABC):
    def __init__(self, url: str) -> None:
        self.url = url
        self.parsed = urlparse(url)

    @abstractmethod
    async def get_tasks(self) -> Dict[str, Dict]:
        """Load tasks"""

    async def close(self) -> None:
        """Close the broker on shutdown"""

    @classmethod
    def from_env(cls) -> "Broker":
        url = os.getenv("SCHEDULER_BROKER_URL", "")
        p = urlparse(url)
        Factory = _brokers.get(p.scheme)
        if not Factory:
            raise RuntimeError(f"Inavalid broker {url}")
        return Factory(url)

    @classmethod
    def register_broker(cls, name: str, factory: Callable):
        _brokers[name] = factory


class RedisBroker(Broker):
    async def get_tasks(self) -> Dict[str, Dict]:
        return {}


Broker.register_broker("redis", RedisBroker)
