from __future__ import annotations

import time
from datetime import date, datetime, timezone


NANOS_PER_MICROS = 1000
NANOS_PER_MILLIS = 1000 * NANOS_PER_MICROS
NANOS_PER_SECOND = 1000 * NANOS_PER_MILLIS


class Timestamp(int):
    """An UTC timestamp with converstion to and from proto"""

    @property
    def nanos(self) -> int:
        """timestamp as nanoseconds"""
        return self

    @property
    def micros(self) -> int:
        """timestamp as microseconds"""
        return self // NANOS_PER_MICROS

    @property
    def millis(self) -> int:
        """timestamp as milliseconds"""
        return self // NANOS_PER_MILLIS

    @property
    def seconds(self) -> int:
        """timestamp as seconds"""
        return self // NANOS_PER_SECOND

    @property
    def total_seconds(self) -> float:
        return self / NANOS_PER_SECOND

    @classmethod
    def utcnow(cls) -> Timestamp:
        return cls(time.time_ns())

    @classmethod
    def utcnow_plus_millis(cls, millis: int) -> Timestamp:
        return cls(time.time_ns() + millis * NANOS_PER_MILLIS)

    @classmethod
    def from_micros(cls, micros: int) -> Timestamp:
        return cls(micros * NANOS_PER_MICROS)

    @classmethod
    def from_millis(cls, millis: int | float) -> Timestamp:
        return cls(millis * NANOS_PER_MILLIS)

    @classmethod
    def from_seconds(cls, seconds: float) -> Timestamp:
        return cls(seconds * NANOS_PER_SECOND)

    @classmethod
    def from_datetime(cls, dte: date) -> Timestamp:
        if not isinstance(dte, datetime):
            dte = datetime.combine(dte, datetime.min.time())
        return cls(int(dte.timestamp() * NANOS_PER_SECOND))

    @classmethod
    def from_datetime_or_none(cls, dte: date | None) -> Timestamp | None:
        if dte is None:
            return None
        return cls.from_datetime(dte)

    @classmethod
    def from_iso_string(cls, dte: str) -> Timestamp:
        return cls.from_datetime(datetime.fromisoformat(dte))

    def to_datetime(self, tzinfo: timezone | None = timezone.utc) -> datetime:
        return datetime.fromtimestamp(self.total_seconds, tz=tzinfo)

    def millis_diff(self, other: Timestamp) -> float:
        return (self.nanos - other.nanos) / NANOS_PER_MILLIS

    def diff(self, other: Timestamp) -> Timestamp:
        return Timestamp(self.nanos - other.nanos)

    def add_millis(self, millis: float | int) -> Timestamp:
        return Timestamp(self.nanos + millis * NANOS_PER_MILLIS)
