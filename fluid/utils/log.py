import logging
from logging.config import dictConfig
from typing import Any, Sequence

try:
    import pythonjsonlogger
except ImportError:
    pythonjsonlogger = None  # type: ignore

from fluid import settings

logger = logging.getLogger()


logger = logging.getLogger(settings.APP_NAME)


def get_logger(name: str = "", prefix: bool = False) -> logging.Logger:
    if prefix:
        return logger.getChild(name) if name else logger
    else:
        return logging.getLogger(name) if name else logger


def level_num(level: str) -> int:
    return getattr(logging, level.upper())


def log_config(
    level: int,
    other_level: int = logging.WARNING,
    app_names: Sequence[str] = (settings.APP_NAME,),
    log_handler: str = settings.LOG_HANDLER,
    log_format: str = settings.PYTHON_LOG_FORMAT,
    formatters: dict[str, dict[str, str]] | None = None,
) -> dict:
    other_level = max(level, other_level)
    log_handlers = {
        "plain": {
            "level": level,
            "class": "logging.StreamHandler",
            "formatter": "plain",
        }
    }
    log_formatters: dict = {"plain": {"format": log_format}}
    if pythonjsonlogger is not None:
        log_handlers.update(
            json={
                "level": level,
                "class": "logging.StreamHandler",
                "formatter": "json",
            },
            nicejson={
                "level": level,
                "class": "logging.StreamHandler",
                "formatter": "nicejson",
            },
        )
        log_formatters.update(
            json={
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": log_format,
            },
            nicejson={
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": log_format,
                "json_indent": 2,
            },
        )
    if formatters:
        log_formatters.update(formatters)
    app_name_set = set(app_names)
    if settings.APP_NAME not in app_name_set:
        app_name_set.add(settings.APP_NAME)
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": log_formatters,
        "handlers": log_handlers,
        "loggers": {
            app_name: {"level": level, "handlers": [log_handler], "propagate": 0}
            for app_name in app_name_set
        },
        "root": {"level": other_level, "handlers": [log_handler]},
    }


def config(**kwargs: Any) -> dict:
    cfg = log_config(level_num(settings.LOG_LEVEL), **kwargs)
    dictConfig(cfg)
    return cfg
