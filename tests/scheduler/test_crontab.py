from datetime import datetime

import pytest

from fluid.scheduler import crontab
from fluid.utils.dates import as_utc


def test_crontab_month() -> None:
    # validates the following months, 1, 4, 7, 8, 9
    valids = set((1, 4, 7, 8, 9))
    validate_m = crontab(month="1,4,*/6,8-9")
    assert str(validate_m) == (
        "crontab(minute *; hour *; day *; month 1,4,*/6,8-9; day_of_week *)"
    )

    for x in range(1, 13):
        res = validate_m(as_utc(datetime(2011, x, 1)))
        if x in valids:
            assert res
        else:
            assert res is None


def test_crontab_day() -> None:
    # validates the following days
    valids = [1, 4, 7, 8, 9, 13, 19, 25, 31]
    validate_d = crontab(day="*/6,1,4,8-9")

    for x in range(1, 32):
        res = validate_d(as_utc(datetime(2011, 1, x)))
        if x in valids:
            assert res
        else:
            assert res is None

    valids = [1, 11, 21, 31]
    validate_d = crontab(day="*/10")
    for x in range(1, 32):
        res = validate_d(as_utc(datetime(2011, 1, x)))
        if x in valids:
            assert res
        else:
            assert res is None

    valids.pop()  # Remove 31, as feb only has 28 days.
    for x in range(1, 29):
        res = validate_d(as_utc(datetime(2011, 2, x)))
        if x in valids:
            assert res
        else:
            assert res is None


def test_crontab_hour() -> None:
    # validates the following hours
    valids = set((0, 1, 4, 6, 8, 9, 12, 18))
    validate_h = crontab(hour="8-9,*/6,1,4")

    for x in range(24):
        res = validate_h(as_utc(datetime(2011, 1, 1, x)))
        if x in valids:
            assert res
        else:
            assert res is None

    edge = crontab(hour=0)
    assert edge(as_utc(datetime(2011, 1, 1, 0, 0)))
    assert edge(as_utc(datetime(2011, 1, 1, 12, 0))) is None


def test_crontab_minute() -> None:
    # validates the following minutes
    valids = set((0, 1, 4, 6, 8, 9, 12, 18, 24, 30, 36, 42, 48, 54))
    validate_m = crontab(minute="4,8-9,*/6,1")

    for x in range(60):
        res = validate_m(as_utc(datetime(2011, 1, 1, 1, x)))
        if x in valids:
            assert res
        else:
            assert res is None

    # We don't ensure *every* X minutes, but just on the given intervals.
    valids = set((0, 16, 32, 48))
    validate_m = crontab(minute="*/16")
    for x in range(60):
        res = validate_m(as_utc(datetime(2011, 1, 1, 1, x)))
        if x in valids:
            assert res
        else:
            assert res is None


def test_crontab_day_of_week() -> None:
    # validates the following days of week
    # jan, 1, 2011 is a saturday
    valids = set((2, 4, 9, 11, 16, 18, 23, 25, 30))
    validate_dow = crontab(day_of_week="0,2")

    for x in range(1, 32):
        res = validate_dow(as_utc(datetime(2011, 1, x)))
        if x in valids:
            assert res
        else:
            assert res is None


def test_crontab_sunday() -> None:
    for dow in ("0", "7"):
        validate = crontab(day_of_week=dow, hour="0", minute="0")
        valid = set((2, 9, 16, 23, 30))
        for x in range(1, 32):
            if x in valid:
                assert validate(as_utc(datetime(2011, 1, x)))
            else:
                assert validate(as_utc(datetime(2011, 1, x))) is None


def test_crontab_all_together() -> None:
    # jan 1, 2011 is a saturday
    # may 1, 2011 is a sunday
    validate = crontab(
        month="1,5", day="1,4,7", day_of_week="0,6", hour="*/4", minute="1-5,10-15,50"
    )

    assert validate(as_utc(datetime(2011, 5, 1, 4, 11)))
    assert validate(as_utc(datetime(2011, 5, 7, 20, 50)))
    assert validate(as_utc(datetime(2011, 1, 1, 0, 1)))

    # fails validation on month
    assert validate(as_utc(datetime(2011, 6, 4, 4, 11))) is None

    # fails validation on day
    assert validate(as_utc(datetime(2011, 1, 6, 4, 11))) is None

    # fails validation on day_of_week
    assert validate(as_utc(datetime(2011, 1, 4, 4, 11))) is None

    # fails validation on hour
    assert validate(as_utc(datetime(2011, 1, 1, 1, 11))) is None

    # fails validation on minute
    assert validate(as_utc(datetime(2011, 1, 1, 4, 6))) is None


def test_invalid_crontabs() -> None:
    # check invalid configurations are detected and reported
    with pytest.raises(ValueError):
        crontab(minute="61")
    with pytest.raises(ValueError):
        crontab(minute="0-61")
    with pytest.raises(ValueError):
        crontab(day_of_week="*/3")


async def test_consecutive_runs() -> None:
    schedule = crontab(day="*", hour=8, minute=0)
    run = schedule(as_utc(datetime(2021, 2, 20, 8)))
    assert run
    # seconds are not considered in crontab scheduler
    assert schedule(as_utc(datetime(2021, 2, 20, 8)), run) is None
    assert schedule(as_utc(datetime(2021, 2, 20, 8, 0, 1)), run) is None
    assert schedule(as_utc(datetime(2021, 2, 20, 8, 1, 0)), run) is None
