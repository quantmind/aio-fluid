# Settings

Process-wide configuration, sourced from environment variables. Settings cover the task consumer concurrency and timings, the broker and database connections, the HTTP client user agent, pagination defaults, the console backdoor and the stack sampler.

It can be imported from `fluid.settings`:

```python
from fluid.settings import get_settings

settings = get_settings()
settings.max_concurrent_tasks
```

The environment is read the first time get_settings is called, not at import time, so an application can populate the environment before the first access. The instance is then cached for the lifetime of the process.

## Environment variable names

Most fields are read from `FLUID_<FIELD_NAME>`, and names are case insensitive, so `FLUID_MAX_CONCURRENT_TASKS` and `fluid_max_concurrent_tasks` both set `max_concurrent_tasks`.

A few fields keep a conventional external name instead, with no prefix:

| Field                   | Environment variable    |
| ----------------------- | ----------------------- |
| `app_name`              | `APP_NAME`              |
| `env`                   | `PYTHON_ENV`            |
| `log_level`             | `LOG_LEVEL`             |
| `log_handler`           | `LOG_HANDLER`           |
| `python_log_format`     | `PYTHON_LOG_FORMAT`     |
| `database`              | `DATABASE`              |
| `redis_default_url`     | `REDIS_DEFAULT_URL`     |
| `redis_max_connections` | `MAX_REDIS_CONNECTIONS` |

Warning

The prefixed form does not work for the fields in the table above. Setting `FLUID_APP_NAME` has no effect, the value is read from `APP_NAME` only.

The prefix itself can be changed with `FLUID_ENV_PREFIX`, which is read when `fluid.settings` is imported, so it has to be set before the first import of the library:

```bash
FLUID_ENV_PREFIX=svc_ SVC_MAX_CONCURRENT_TASKS=10 python -m myapp serve
```

## Derived defaults

Three values are computed after the environment is read, when they are not set explicitly:

- `broker_url` falls back to `redis_default_url`, so pointing `REDIS_DEFAULT_URL` at a Redis instance is enough to move the task queue with it.
- `http_user_agent` falls back to `python/{app_name}`.
- `log_level` is upper cased, so `LOG_LEVEL=info` and `LOG_LEVEL=INFO` are equivalent.

## Reading settings in tests

get_settings caches its result, so a test that changes the environment has to clear the cache for the change to take effect:

```python
import os

from fluid.settings import get_settings

os.environ["FLUID_MAX_CONCURRENT_TASKS"] = "1"
get_settings.cache_clear()
```

## API reference

## fluid.settings.Settings

Bases: `BaseSettings`

Lazy application settings sourced from environment variables.

Settings are read from the environment the first time get_settings is called, not at import time. Access the resolved values either via the cached instance or, for backwards compatibility, via upper-case module attributes (`settings.APP_NAME`), both of which resolve lazily.

### model_config

```python
model_config = SettingsConfigDict(
    case_sensitive=False,
    extra="ignore",
    env_prefix=ENV_PREFIX,
)
```

### app_name

```python
app_name = Field(
    default="fluid", validation_alias="APP_NAME"
)
```

### env

```python
env = Field(default='dev', validation_alias='PYTHON_ENV')
```

### log_level

```python
log_level = Field(
    default="info", validation_alias="LOG_LEVEL"
)
```

### log_handler

```python
log_handler = Field(
    default="plain", validation_alias="LOG_HANDLER"
)
```

### python_log_format

```python
python_log_format = Field(
    default="%(asctime)s %(levelname)s %(name)s %(message)s",
    validation_alias="PYTHON_LOG_FORMAT",
)
```

### database

```python
database = Field(
    default="postgresql+asyncpg://postgres:postgres@localhost:5432/fluid",
    validation_alias="DATABASE",
)
```

### redis_default_url

```python
redis_default_url = Field(
    default="redis://localhost:6379",
    validation_alias="REDIS_DEFAULT_URL",
)
```

### stopping_grace_period

```python
stopping_grace_period = 10
```

### max_concurrent_tasks

```python
max_concurrent_tasks = Field(
    default=5,
    description="Maximum number of concurrent tasks per TaskConsumer",
)
```

### sleep_millis

```python
sleep_millis = 1000
```

### scheduler_heartbeat_millis

```python
scheduler_heartbeat_millis = 100
```

### broker_url

```python
broker_url = ''
```

### redis_max_connections

```python
redis_max_connections = Field(
    default=5, validation_alias="MAX_REDIS_CONNECTIONS"
)
```

### database_schema

```python
database_schema = None
```

### dbpool_max_size

```python
dbpool_max_size = 10
```

### dbpool_max_overflow

```python
dbpool_max_overflow = 10
```

### dbecho

```python
dbecho = False
```

### http_user_agent

```python
http_user_agent = ''
```

### default_pagination_limit

```python
default_pagination_limit = 250
```

### default_pagination_max_limit

```python
default_pagination_max_limit = 500
```

### backdoor_port

```python
backdoor_port = 8087
```

### flamegraph_executable

```python
flamegraph_executable = 'flamegraph.pl'
```

### stack_sampler_period_seconds

```python
stack_sampler_period_seconds = 1
```

## fluid.settings.get_settings

```python
get_settings()
```

Return the process-wide Settings instance.

The instance is built on first call (reading the environment then) and cached for the lifetime of the process. Call `get_settings.cache_clear()` to force a re-read, which is mostly useful in tests.

Source code in `fluid/settings.py`

```python
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide [Settings][fluid.settings.Settings] instance.

    The instance is built on first call (reading the environment then) and
    cached for the lifetime of the process. Call ``get_settings.cache_clear()``
    to force a re-read, which is mostly useful in tests.
    """
    return Settings()
```
