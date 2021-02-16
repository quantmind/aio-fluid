import logging
import os
from logging.config import dictConfig
from typing import Dict

import click
from openapi.logger import LOGGER_NAME, colorlog

logger = logging.getLogger()


APP_NAME = LOGGER_NAME or "fluid"


LOG_FORMAT_PRODUCTION = (
    "%(asctime)s %(name)s %(levelname)s %(filename)s %(funcName)s "
    "%(lineno)s %(module)s %(threadName)s %(message)s"
)

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def level_num(level: str) -> int:
    return getattr(logging, level)


def log_config(level: str) -> Dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": LOG_FORMAT_PRODUCTION,
            },
            "verbose": {"format": LOG_FORMAT_PRODUCTION},
        },
        "handlers": {
            "main": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "json",
            }
        },
        "loggers": {APP_NAME: {"level": level, "handlers": ["main"], "propagate": 0}},
        "root": {"level": "INFO", "handlers": ["main"]},
    }


def log_name(name: str = "") -> str:
    return APP_NAME if not name else f"{APP_NAME}.{name}"


def setup(level: str):
    level = level_num(level)
    if level > logging.INFO or os.environ.get("KUBERNETES_SERVICE_HOST"):
        dictConfig(log_config(level))
    else:
        logger.setLevel(level)
        if not logger.hasHandlers():
            fmt = LOG_FORMAT
            if colorlog:
                handler = colorlog.StreamHandler()
                fmt = colorlog.ColoredFormatter(f"%(log_color)s{LOG_FORMAT}")
            else:  # pragma: no cover
                handler = logging.StreamHandler()
                fmt = logging.Formatter(LOG_FORMAT)
            handler.setFormatter(fmt)
            logger.addHandler(handler)


@click.pass_context
def setup_logging(ctx: click.Context, verbose: bool, quiet: bool) -> None:
    if verbose:
        level = "DEBUG"
    elif quiet:
        level = "WARNING"
    else:
        level = "INFO"
    ctx.obj["log_level"] = level_num(level)
    setup(level)
