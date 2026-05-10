<h1>
  <a href="https://fluid.quantmind.com/">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://fluid.quantmind.com/assets/fluid-banner-light.svg" style="max-width: 400px; width: 100%;">
      <img alt="aio-fluid — async utilities for backend Python services" src="https://fluid.quantmind.com/assets/fluid-banner-dark.svg" style="max-width: 400px; width: 100%;">
    </picture>
  </a>
</h1>

Async utilities for backend python services developed by [Quantmind](https://quantmind.com).

[![PyPI version](https://badge.fury.io/py/aio-fluid.svg)](https://badge.fury.io/py/aio-fluid)
[![Python versions](https://img.shields.io/pypi/pyversions/aio-fluid.svg)](https://pypi.org/project/aio-fluid)
[![Python downloads](https://static.pepy.tech/badge/aio-fluid/month)](https://pepy.tech/project/aio-fluid)
[![build](https://github.com/quantmind/fluid/workflows/build/badge.svg)](https://github.com/quantmind/aio-fluid/actions?query=workflow%3Abuild)
[![codecov](https://codecov.io/gh/quantmind/aio-fluid/graph/badge.svg?token=81oWUoyEVp)](https://codecov.io/gh/quantmind/aio-fluid)

**Documentation**: [fluid.quantmind.com](https://fluid.quantmind.com/)

**Source Code**: [github.com/quantmind/aio-fluid](https://github.com/quantmind/aio-fluid)


## Features

- **Async workers** — composable components with a managed start/stop lifecycle; the building block everything else is built on. See [Workers](https://fluid.quantmind.com/reference/workers/).
- **Task scheduler & consumer** — declare tasks with a `@task` decorator, schedule them with `every()` or cron expressions, run them concurrently with priorities and concurrency limits, and offload CPU-bound work to subprocesses or Kubernetes Jobs. Backed by a pluggable broker (Redis by default). See [Tasks](https://fluid.quantmind.com/reference/task/).
- **Async Postgres CRUD** — a typed CRUD layer over `asyncpg` and SQLAlchemy, with pagination and schema migrations. See [Database](https://fluid.quantmind.com/reference/db/).
- **Event dispatchers** — sync and async `Dispatcher` types for decoupling event sources from handlers. See [Dispatchers](https://fluid.quantmind.com/reference/dispatchers/).
- **HTTP client helpers** — a unified async client wrapping `httpx` and `aiohttp`. See [HTTP Client](https://fluid.quantmind.com/reference/http_client/).
- **CLI tooling** — ready-made `click` / `rich` command-line interfaces for task managers and databases.

## Installation

This is a python package you can install via pip:

```
pip install aio-fluid
```

To install all the dependencies:

```
pip install aio-fluid[cli, db, http, log, k8s]
```
this includes the following extra dependencies:

- `cli` for the command line interface using [click](https://click.palletsprojects.com/) and [rich](https://github.com/Textualize/rich)
- `db` for database support with [asyncpg](https://github.com/MagicStack/asyncpg) and [sqlalchemy](https://www.sqlalchemy.org/)
- `http` for http client support with [httpx](https://www.python-httpx.org/) and [aiohttp](https://docs.aiohttp.org/en/stable/)
- `log` for JSON logging support with [python-json-logger](https://github.com/madzak/python-json-logger)
- `k8s` for Kubernetes support for CPU bound tasks

## Development

You can run the examples via

```
uv run python -m examples
```

We use [uv](https://uv.run/) as a development tool to run the examples and tests, but you can also use python directly if that's your preference.

## License

This project is licensed under the BSD License - see the [LICENSE](https://github.com/quantmind/aio-fluid/blob/main/LICENSE) file for details.
