from datetime import date, datetime, timezone
from typing import Any


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def as_utc(dt: date | None) -> datetime:
    if dt is None:
        return utcnow()
    elif isinstance(dt, datetime):
        return dt.replace(tzinfo=timezone.utc)
    else:
        return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


def isoformat(dt: datetime, **kwargs: Any) -> str:
    if dt.tzinfo is timezone.utc:
        return dt.replace(tzinfo=None).isoformat(**kwargs) + "Z"
    else:
        return dt.isoformat(**kwargs)
