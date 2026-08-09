# Settings

Process-wide configuration, sourced from environment variables. Settings cover the
task consumer concurrency and timings, the broker and database connections, the HTTP
client user agent, pagination defaults, the console backdoor and the stack sampler.

It can be imported from `fluid.settings`:

```python
from fluid.settings import get_settings

settings = get_settings()
settings.max_concurrent_tasks
```

The environment is read the first time [get_settings][fluid.settings.get_settings] is
called, not at import time, so an application can populate the environment before the
first access. The instance is then cached for the lifetime of the process.

## Environment variable names

Most fields are read from `FLUID_<FIELD_NAME>`, and names are case insensitive, so
`FLUID_MAX_CONCURRENT_TASKS` and `fluid_max_concurrent_tasks` both set
`max_concurrent_tasks`.

A few fields keep a conventional external name instead, with no prefix:

| Field | Environment variable |
|---|---|
| `app_name` | `APP_NAME` |
| `env` | `PYTHON_ENV` |
| `log_level` | `LOG_LEVEL` |
| `log_handler` | `LOG_HANDLER` |
| `python_log_format` | `PYTHON_LOG_FORMAT` |
| `database` | `DATABASE` |
| `redis_default_url` | `REDIS_DEFAULT_URL` |
| `redis_max_connections` | `MAX_REDIS_CONNECTIONS` |

!!! warning
    The prefixed form does not work for the fields in the table above. Setting
    `FLUID_APP_NAME` has no effect, the value is read from `APP_NAME` only.

The prefix itself can be changed with `FLUID_ENV_PREFIX`, which is read when
`fluid.settings` is imported, so it has to be set before the first import of the
library:

```bash
FLUID_ENV_PREFIX=svc_ SVC_MAX_CONCURRENT_TASKS=10 python -m myapp serve
```

## Derived defaults

Three values are computed after the environment is read, when they are not set
explicitly:

- `broker_url` falls back to `redis_default_url`, so pointing `REDIS_DEFAULT_URL` at a
  Redis instance is enough to move the task queue with it.
- `http_user_agent` falls back to `python/{app_name}`.
- `log_level` is upper cased, so `LOG_LEVEL=info` and `LOG_LEVEL=INFO` are equivalent.

## Reading settings in tests

[get_settings][fluid.settings.get_settings] caches its result, so a test that changes
the environment has to clear the cache for the change to take effect:

```python
import os

from fluid.settings import get_settings

os.environ["FLUID_MAX_CONCURRENT_TASKS"] = "1"
get_settings.cache_clear()
```

## API reference

::: fluid.settings.Settings

::: fluid.settings.get_settings
