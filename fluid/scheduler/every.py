from datetime import datetime, timedelta
from typing import Optional

from .crontab import CronRun, Scheduler


class every(Scheduler):
    def __init__(self, delta: timedelta, delay: timedelta = timedelta()):
        self.delta = delta
        self.delay = delay
        self._started = None

    def info(self) -> str:
        return str(self.delta)

    def __call__(
        self, timestamp: datetime, last_run: Optional[CronRun] = None
    ) -> Optional[CronRun]:
        if not last_run and self.delay:
            if not self._started:
                self._started = timestamp
            if timestamp - self._started < self.delay:
                return None
        year, month, day, hour, minute, second, _, _, _ = timestamp.timetuple()
        run = CronRun(year, month, day, hour, minute, second)
        if not last_run or timestamp - last_run.datetime >= self.delta:
            return run
        return None
