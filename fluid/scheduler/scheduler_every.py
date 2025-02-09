import random
from datetime import datetime, timedelta

from .scheduler_crontab import CronRun, Scheduler


class every(Scheduler):  # noqa: N801
    def __init__(
        self,
        delta: timedelta,
        delay: timedelta = timedelta(),
        jitter: timedelta = timedelta(),
    ) -> None:
        self.delta: timedelta = delta
        self.delay: timedelta = delay
        self.jitter: timedelta = jitter
        self._delta: timedelta = self.next_delta()
        self._started: datetime | None = None

    def info(self) -> str:
        return str(self.delta)

    def __call__(
        self, timestamp: datetime, last_run: CronRun | None = None
    ) -> CronRun | None:
        if not last_run and self.delay:
            if not self._started:
                self._started = timestamp
            if timestamp - self._started < self.delay:
                return None
        year, month, day, hour, minute, second, _, _, _ = timestamp.timetuple()
        run = CronRun(year, month, day, hour, minute, second)
        if not last_run or timestamp - last_run.datetime >= self._delta:
            self._delta = self.next_delta()
            return run
        return None

    def next_delta(self) -> timedelta:
        return self.delta + random.uniform(0, 1) * self.jitter
