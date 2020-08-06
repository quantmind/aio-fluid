import os

from openapi import sentry
from openapi.middleware import json_error

PYTHON_ENV = os.environ.get("PYTHON_ENV", "production")
ENVIRONMENT = os.environ.get("ENVIRONMENT", PYTHON_ENV)
SENTRY_DSN = os.environ.get("SENTRY_DSN", "")


def add_error_middleware(app):
    app["env"] = PYTHON_ENV
    sm = sentry.middleware(app, SENTRY_DSN, ENVIRONMENT)
    app.middlewares.append(json_error())
    app.middlewares.append(sm)
