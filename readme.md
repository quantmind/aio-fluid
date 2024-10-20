Aio Fluid

Async utilities for backend python services developed by [Quantmind](https://quantmind.com).

[![PyPI version](https://badge.fury.io/py/aio-fluid.svg)](https://badge.fury.io/py/aio-fluid)
[![Python versions](https://img.shields.io/pypi/pyversions/aio-fluid.svg)](https://pypi.org/project/aio-fluid)
[![build](https://github.com/quantmind/fluid/workflows/build/badge.svg)](https://github.com/quantmind/aio-fluid/actions?query=workflow%3Abuild)
[![codecov](https://codecov.io/gh/quantmind/aio-fluid/graph/badge.svg?token=81oWUoyEVp)](https://codecov.io/gh/quantmind/aio-fluid)

**Documentation**: [https://quantmind.github.io/aio-fluid](https://quantmind.github.io/aio-fluid)

**Source Code**: [https://github.com/quantmind/aio-fluid](https://github.com/quantmind/aio-fluid)


## Installation

This is a simple python package you can install via pip:

```
pip install aio-fluid
```

To install all the dependencies:

```
pip install aio-fluid[cli, db, http, log]
```
this includes the following extra dependencies:

- `cli` for the command line interface using [click](https://click.palletsprojects.com/) and [rich](https://github.com/Textualize/rich)
- `db` for database support with [asyncpg](https://github.com/MagicStack/asyncpg) and [sqlalchemy](https://www.sqlalchemy.org/)
- `http` for http client support with [httpx](https://www.python-httpx.org/) and [aiohttp](https://docs.aiohttp.org/en/stable/)
- `log` for JSON logging support with [python-json-logger](https://github.com/madzak/python-json-logger)


## Development

You can run the examples via

```
poetry run python -m examples.main
```
