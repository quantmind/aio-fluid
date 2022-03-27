from datetime import datetime, timedelta
from typing import Optional

from .crontab import CronRun, Scheduler


class every(Scheduler):
    def __init__(self, delta: timedelta):
        self.delta = delta

    def info(self) -> str:
        return str(self.delta)

    def __call__(
        self, timestamp: datetime, last_run: Optional[CronRun] = None
    ) -> Optional[CronRun]:
        year, month, day, hour, minute, second, _, _, _ = timestamp.timetuple()
        run = CronRun(year, month, day, hour, minute, second)
        if not last_run or timestamp - last_run.datetime >= self.delta:
            return run
        return None
