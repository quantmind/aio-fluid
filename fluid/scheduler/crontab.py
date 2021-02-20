"""Originally from https://github.com/coleifer/huey
"""
import re
from datetime import datetime
from typing import Callable, NamedTuple, Optional, Union

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

    @property
    def datetime(self) -> datetime:
        return datetime(*self)


ScheduleType = Callable[[datetime, CronRun], Optional[CronRun]]


def crontab(
    minute: CI = "*",
    hour: CI = "*",
    day: CI = "*",
    month: CI = "*",
    day_of_week: CI = "*",
) -> ScheduleType:
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
    validation = (
        ("m", month, range(1, 13)),
        ("d", day, range(1, 32)),
        ("w", day_of_week, range(8)),  # 0-6, but also 7 for Sunday.
        ("H", hour, range(24)),
        ("M", minute, range(60)),
    )
    cron_settings = []

    for (date_str, value, acceptable) in validation:
        settings = set()

        if isinstance(value, int):
            value = str(value)

        for piece in value.split(","):
            if piece == "*":
                settings.update(acceptable)
                continue

            if piece.isdigit():
                piece = int(piece)
                if piece not in acceptable:
                    raise ValueError("%d is not a valid input" % piece)
                elif date_str == "w":
                    piece %= 7
                settings.add(piece)

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
                            "Cannot perform this kind of matching" " on day-of-week."
                        )
                    interval = int(every_match.groups()[0])
                    settings.update(acceptable[::interval])

        cron_settings.append(sorted(list(settings)))

    cron_settings = tuple(cron_settings)

    def validate_date(
        timestamp: datetime, last_run: Optional[CronRun] = None
    ) -> Optional[CronRun]:
        year, month, day, hour, minute, _, w, _, _ = timestamp.timetuple()
        run = CronRun(year, month, day, (w + 1) % 7, hour, minute)
        if last_run == run:
            return None

        for (date_piece, selection) in zip(run[1:], cron_settings):
            if date_piece not in selection:
                return None
        return run

    return validate_date
