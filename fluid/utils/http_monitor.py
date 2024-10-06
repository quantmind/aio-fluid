from __future__ import annotations

from typing import TYPE_CHECKING, Callable
from urllib.parse import urlparse

from prometheus_client import Histogram

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration seconds",
    ["host", "path", "method", "status"],
)

if TYPE_CHECKING:
    from .http_client import HttpResponse


HttpPathFn = Callable[[str], str]


def monitor_http_call(
    response: HttpResponse,
    duration: float,
    sanitization_fn: HttpPathFn | None = None,
) -> None:
    p = urlparse(response.url)

    path = p.path if not sanitization_fn else sanitization_fn(p.path)
    http_request_duration_seconds.labels(
        p.netloc, path, response.method, str(response.status_code)
    ).observe(duration)
