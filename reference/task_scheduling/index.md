# Task Scheduling

Scheduling functions for tasks. They can be imported from `fluid.scheduler`:

```python
from fluid.scheduler import every, crontab
```

## fluid.scheduler.Scheduler

Bases: `ABC`

Base class for all schedulers.

### info

```python
info()
```

Return a string representation of the schedule.

Source code in `fluid/scheduler/scheduler_crontab.py`

```python
@abstractmethod
def info(self) -> str:
    """Return a string representation of the schedule."""
```

## fluid.scheduler.every

```python
every(delta, delay=timedelta(), jitter=timedelta())
```

Bases: `Scheduler`

Run a task every delta time, with optional delay and jitter

| PARAMETER | DESCRIPTION                                                                                   |
| --------- | --------------------------------------------------------------------------------------------- |
| `delta`   | The time delta between runs **TYPE:** `timedelta`                                             |
| `delay`   | The initial delay before the first run **TYPE:** `timedelta` **DEFAULT:** `timedelta()`       |
| `jitter`  | The maximum random jitter added to the delta **TYPE:** `timedelta` **DEFAULT:** `timedelta()` |

Source code in `fluid/scheduler/scheduler_every.py`

```python
def __init__(
    self,
    delta: Annotated[
        timedelta,
        Doc("The time delta between runs"),
    ],
    delay: Annotated[
        timedelta,
        Doc("The initial delay before the first run"),
    ] = timedelta(),
    jitter: Annotated[
        timedelta,
        Doc("The maximum random jitter added to the delta"),
    ] = timedelta(),
) -> None:
    self.delta: timedelta = delta
    self.delay: timedelta = delay
    self.jitter: timedelta = jitter
    self._delta: timedelta = self.next_delta()
    self._started: datetime | None = None
```

### delta

```python
delta = delta
```

### delay

```python
delay = delay
```

### jitter

```python
jitter = jitter
```

### info

```python
info()
```

Source code in `fluid/scheduler/scheduler_every.py`

```python
def info(self) -> str:
    return str(self.delta)
```

### next_delta

```python
next_delta()
```

Source code in `fluid/scheduler/scheduler_every.py`

```python
def next_delta(self) -> timedelta:
    return self.delta + random.uniform(0, 1) * self.jitter
```

## fluid.scheduler.crontab

```python
crontab(
    minute="*",
    hour="*",
    day="*",
    month="*",
    day_of_week="*",
    tz=utc,
)
```

Bases: `Scheduler`

Convert a "crontab"-style set of parameters into a test function that will return True when the given datetime matches the parameters set forth in the crontab. For day-of-week, 0=Sunday and 6=Saturday. Acceptable inputs: * = every distinct value */n = run every "n" times, i.e. hours='*/4' == 0, 4, 8, 12, 16, 20 m-n = run every time m..n m,n = run on m and n

Source code in `fluid/scheduler/scheduler_crontab.py`

```python
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
```

### tz

```python
tz = tz
```

### cron_settings

```python
cron_settings = tuple(cron_settings)
```

### info

```python
info()
```

Source code in `fluid/scheduler/scheduler_crontab.py`

```python
def info(self) -> str:
    return self._info
```
