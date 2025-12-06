import logging
from logging.config import dictConfig
from typing import Sequence

from typing_extensions import Annotated, Doc

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


def get_level_num(level: str | int) -> int:
    if isinstance(level, int):
        return level
    return getattr(logging, level.upper())


def log_config(
    level: str | int = settings.LOG_LEVEL,
    other_level: str | int = logging.WARNING,
    app_names: Sequence[str] = (settings.APP_NAME,),
    log_handler: str = settings.LOG_HANDLER,
    log_format: str = settings.PYTHON_LOG_FORMAT,
    formatters: dict[str, dict[str, str]] | None = None,
) -> dict:
    level_num = get_level_num(level)
    other_level_num = max(level_num, get_level_num(other_level))
    log_handlers = {
        "plain": {
            "level": level_num,
            "class": "logging.StreamHandler",
            "formatter": "plain",
        }
    }
    log_formatters: dict = {"plain": {"format": log_format}}
    if pythonjsonlogger is not None:
        log_handlers.update(
            json={
                "level": level_num,
                "class": "logging.StreamHandler",
                "formatter": "json",
            },
            nicejson={
                "level": level_num,
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
            app_name: {"level": level_num, "handlers": [log_handler], "propagate": 0}
            for app_name in app_name_set
        },
        "root": {"level": other_level_num, "handlers": [log_handler]},
    }


def config(
    level: Annotated[
        str | int,
        Doc(
            "Log levels for application loggers defined by the `app_names` parameter. "
            "By default this value is taken from the `LOG_LEVEL` env variable"
        ),
    ] = settings.LOG_LEVEL,
    other_level: Annotated[
        str | int,
        Doc("log levels for loggers not prefixed by `app_names`"),
    ] = logging.WARNING,
    app_names: Annotated[
        Sequence[str],
        Doc(
            "Application names for which the log level is set, "
            "these are the prefixes which will be set at `log_level`"
        ),
    ] = (settings.APP_NAME,),
    log_handler: Annotated[
        str,
        Doc(
            "Log handler to use, by default it is taken from the "
            "`LOG_HANDLER` env variable and if missing `plain` is used"
        ),
    ] = settings.LOG_HANDLER,
    log_format: Annotated[
        str,
        Doc(
            "log format to use, by default it is taken from the "
            "`PYTHON_LOG_FORMAT` env variable"
        ),
    ] = settings.PYTHON_LOG_FORMAT,
    formatters: Annotated[
        dict[str, dict[str, str]] | None,
        Doc("Additional formatters to add to the logging configuration"),
    ] = None,
) -> dict:
    """Configure logging for the application"""
    cfg = log_config(
        level=level,
        other_level=other_level,
        app_names=app_names,
        log_handler=log_handler,
        log_format=log_format,
        formatters=formatters,
    )
    dictConfig(cfg)
    return cfg
