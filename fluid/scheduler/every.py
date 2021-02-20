from datetime import datetime, timedelta
from typing import Optional

from .crontab import CronRun, ScheduleType


def every(delta: timedelta) -> ScheduleType:
    def validate(
        timestamp: datetime, last_run: Optional[CronRun] = None
    ) -> Optional[CronRun]:
        year, month, day, hour, minute, second, _, _, _ = timestamp.timetuple()
        run = CronRun(year, month, day, hour, minute, second)
        if not last_run or timestamp - last_run.datetime >= delta:
            return run
        return None

    return validate
