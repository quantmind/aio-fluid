# Tools for backend python services

[![PyPI version](https://badge.fury.io/py/aio-fluid.svg)](https://badge.fury.io/py/aio-fluid)
[![build](https://github.com/quantmind/fluid/workflows/build/badge.svg)](https://github.com/quantmind/aio-fluid/actions?query=workflow%3Abuild)
[![codecov](https://codecov.io/gh/quantmind/aio-fluid/branch/master/graph/badge.svg?token=81oWUoyEVp)](https://codecov.io/gh/quantmind/aio-fluid)

## Installation

This is a simple python package you can install via pip:

```
pip install aio-fluid
```

## Modules

### [scheduler](./fluid/scheduler)

A simple asynchronous task queue with a scheduler

### [kernel](./fluid/kernel)

Async utility for executing commands in sub-processes

## AWS

packages for AWS interaction are installed via

- [aiobotocore](https://github.com/aio-libs/aiobotocore)
- [s3fs](https://github.com/fsspec/s3fs) (which depends on aiobotocore and therefore versions must be compatible)
- [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html) is installed as extra dependency of aiobotocore so versioning is compatible
