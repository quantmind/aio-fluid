"""Originally from https://github.com/coleifer/huey"""

import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone, tzinfo
from typing import NamedTuple, Set, Union

dash_re = re.compile(r"(\d+)-(\d+)")
every_re = re.compile(r"\*\/(\d+)")
CI = Union[str, int]


class CronRun(NamedTuple):
    year: int
    month: int
    day: int
    week_day: int
    hour: int
    minute: int
    second: int = 0
    tz: tzinfo = timezone.utc

    @property
    def datetime(self) -> datetime:
        return datetime(*self)


class Scheduler(ABC):
    """Base class for all schedulers."""

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.info()})"

    @abstractmethod
    def info(self) -> str:
        """Return a string representation of the schedule."""

    @abstractmethod
    def __call__(
        self, timestamp: datetime, last_run: CronRun | None = None
    ) -> CronRun | None:
        """Return a CronRun if the given timestamp matches the schedule."""


class crontab(Scheduler):  # noqa N801
    """
    Convert a "crontab"-style set of parameters into a test function that will
    return True when the given datetime matches the parameters set forth in
    the crontab.
    For day-of-week, 0=Sunday and 6=Saturday.
    Acceptable inputs:
    * = every distinct value
    */n = run every "n" times, i.e. hours='*/4' == 0, 4, 8, 12, 16, 20
    m-n = run every time m..n
    m,n = run on m and n
    """

    def __init__(
        self,
        minute: CI = "*",
        hour: CI = "*",
        day: CI = "*",
        month: CI = "*",
        day_of_week: CI = "*",
        tz: tzinfo = timezone.utc,
    ) -> None:
        self.tz: tzinfo = tz
        self._info = (
            f"minute {minute}; hour {hour}; day {day}; month {month}; "
            f"day_of_week {day_of_week}"
        )
        validation = (
            ("m", month, range(1, 13)),
            ("d", day, range(1, 32)),
            ("w", day_of_week, range(8)),  # 0-6, but also 7 for Sunday.
            ("H", hour, range(24)),
            ("M", minute, range(60)),
        )
        cron_settings = []

        for date_str, value, acceptable in validation:
            settings: Set[int] = set()

            if isinstance(value, int):
                value = str(value)

            for piece in value.split(","):
                if piece == "*":
                    settings.update(acceptable)
                    continue

                if piece.isdigit():
                    digit = int(piece)
                    if digit not in acceptable:
                        raise ValueError("%d is not a valid input" % digit)
                    elif date_str == "w":
                        digit %= 7
                    settings.add(digit)

                else:
                    dash_match = dash_re.match(piece)
                    if dash_match:
                        lhs, rhs = map(int, dash_match.groups())
                        if lhs not in acceptable or rhs not in acceptable:
                            raise ValueError("%s is not a valid input" % piece)
                        elif date_str == "w":
                            lhs %= 7
                            rhs %= 7
                        settings.update(range(lhs, rhs + 1))
                        continue

                    # Handle stuff like */3, */6.
                    every_match = every_re.match(piece)
                    if every_match:
                        if date_str == "w":
                            raise ValueError(
                                "Cannot perform this kind of matching"
                                " on day-of-week."
                            )
                        interval = int(every_match.groups()[0])
                        settings.update(acceptable[::interval])

            cron_settings.append(sorted(list(settings)))
        self.cron_settings = tuple(cron_settings)

    def info(self) -> str:
        return self._info

    def __call__(
        self, timestamp: datetime, last_run: CronRun | None = None
    ) -> CronRun | None:
        # assume datetime is always utc
        if timestamp.tzinfo is None:
            raise ValueError("timestamp tick must have a timezone")
        if timestamp.tzinfo != self.tz:
            # convert timestamp into cron_settings timezone
            timestamp = timestamp.astimezone(self.tz)
        year, month, day, hour, minute, _, w, _, _ = timestamp.timetuple()
        run = CronRun(year, month, day, (w + 1) % 7, hour, minute)
        if last_run == run:
            return None

        for date_piece, selection in zip(run[1:], self.cron_settings, strict=False):
            if date_piece not in selection:
                return None
        return run
