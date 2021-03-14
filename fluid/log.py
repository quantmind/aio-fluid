import logging
import os
from logging.config import dictConfig
from typing import Dict

import click

logger = logging.getLogger()


LOG_LEVEL = (os.environ.get("LOG_LEVEL") or "info").upper()
APP_NAME = os.environ.get("APP_NAME") or "fluid"
K8S = os.environ.get("KUBERNETES_SERVICE_HOST")


LOG_FORMAT_PRODUCTION = (
    "%(asctime)s %(name)s %(levelname)s %(filename)s %(funcName)s "
    "%(lineno)s %(module)s %(threadName)s %(message)s"
)

LOG_FORMAT_DEV = "%(asctime)s %(levelname)s %(name)s %(message)s"

LOG_FORMAT = LOG_FORMAT_PRODUCTION if K8S else LOG_FORMAT_DEV


logger = logging.getLogger(APP_NAME)


def get_logger(name: str = "") -> logging.Logger:
    return logger.getChild(name) if name else logger


def level_num(level: str) -> int:
    return getattr(logging, level)


def log_config(level: int) -> Dict:
    other_level = max(level, logging.INFO)
    handler = "main" if K8S else "color"
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": LOG_FORMAT,
            },
            "color": {
                "()": "colorlog.ColoredFormatter",
                "format": f"%(log_color)s{LOG_FORMAT}",
            },
            "verbose": {"format": LOG_FORMAT},
        },
        "handlers": {
            "main": {
                "level": level,
                "class": "logging.StreamHandler",
                "formatter": "json",
            },
            "color": {
                "level": level,
                "class": "colorlog.StreamHandler",
                "formatter": "color",
            },
        },
        "loggers": {
            APP_NAME: {"level": level, "handlers": [handler], "propagate": 0},
        },
        "root": {"level": other_level, "handlers": [handler]},
    }


def log_name(name: str = "") -> str:
    return APP_NAME if not name else f"{APP_NAME}.{name}"


def setup(level: str):
    dictConfig(log_config(level_num(level)))


@click.pass_context
def setup_logging(ctx: click.Context, verbose: bool, quiet: bool) -> None:
    if verbose:
        level = "DEBUG"
    elif quiet:
        level = "WARNING"
    else:
        level = LOG_LEVEL
    ctx.obj["log_level"] = level_num(level)
    setup(level)
