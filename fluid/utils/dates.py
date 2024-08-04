from datetime import datetime, timezone
from typing import Any

from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=UTC)


def isoformat(dt: datetime, **kwargs: Any) -> str:
    if dt.tzinfo is timezone.utc:
        return dt.replace(tzinfo=None).isoformat(**kwargs) + "Z"
    else:
        return dt.isoformat(**kwargs)
