import json
import logging
import warnings

import pytest

from fluid.utils import log

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_get_level_num_from_int() -> None:
    assert log.get_level_num(logging.DEBUG) == logging.DEBUG


async def test_get_level_num_from_string() -> None:
    assert log.get_level_num("info") == logging.INFO
    assert log.get_level_num("WARNING") == logging.WARNING


async def test_config_plain() -> None:
    cfg = log.config(log_handler="plain", level="INFO")
    assert cfg["handlers"]["plain"]["formatter"] == "plain"
    assert cfg["loggers"]["fluid"]["level"] == logging.INFO


async def test_config_other_level_floored_by_level() -> None:
    cfg = log.config(level="ERROR", other_level=logging.WARNING)
    # other loggers cannot be more verbose than the app level
    assert cfg["root"]["level"] == logging.ERROR


async def test_config_app_names() -> None:
    cfg = log.config(app_names=["myapp"], level="INFO")
    # the configured app and the default app name are both present
    assert "myapp" in cfg["loggers"]
    assert "fluid" in cfg["loggers"]


async def test_config_extra_formatters() -> None:
    cfg = log.config(formatters={"custom": {"format": "%(message)s"}})
    assert "custom" in cfg["formatters"]


@pytest.mark.parametrize("handler", ["json", "nicejson"])
async def test_config_json_handlers(
    handler: str, capsys: pytest.CaptureFixture
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        log.config(log_handler=handler, level="INFO")
    logging.getLogger("fluid").info("hello %s", handler)
    captured = capsys.readouterr()
    record = json.loads(captured.err)
    assert record["message"] == f"hello {handler}"
    assert record["levelname"] == "INFO"


async def test_config_json_formatter_uses_new_path() -> None:
    cfg = log.config(log_handler="json")
    assert cfg["formatters"]["json"]["()"] == "pythonjsonlogger.json.JsonFormatter"
