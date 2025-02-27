Aio Fluid

Async utilities for backend python services developed by [Quantmind](https://quantmind.com).

[![PyPI version](https://badge.fury.io/py/aio-fluid.svg)](https://badge.fury.io/py/aio-fluid)
[![Python versions](https://img.shields.io/pypi/pyversions/aio-fluid.svg)](https://pypi.org/project/aio-fluid)
[![Python downloads](https://img.shields.io/pypi/dd/aio-fluid.svg)](https://pypi.org/project/aio-fluid)
[![build](https://github.com/quantmind/fluid/workflows/build/badge.svg)](https://github.com/quantmind/aio-fluid/actions?query=workflow%3Abuild)
[![codecov](https://codecov.io/gh/quantmind/aio-fluid/graph/badge.svg?token=81oWUoyEVp)](https://codecov.io/gh/quantmind/aio-fluid)

**Documentation**: [fluid.quantmind.com](https://fluid.quantmind.com/)

**Source Code**: [github.com/quantmind/aio-fluid](https://github.com/quantmind/aio-fluid)


## Features

- **Async workers**: workers with start/stop capabilities.
- **Async tasks scheduler and consumer**: A task scheduler and consumer for async and CPU bound tasks.
- **Async CRUD database operations**: An async CRUD interface for postgres databases.

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
poetry run python -m examples.main
```
