"""Tests for the command which executes a cpu bound task in a separate process"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, SecretStr

from fluid.scheduler.cpubound import cpu_bound_command, exec_args, strip_serve

pytestmark = pytest.mark.asyncio(loop_scope="function")


class NoParams(BaseModel):
    pass


class SecretParams(BaseModel):
    token: SecretStr


def make_ctx(
    name: str = "heavy_calc", run_id: str = "abc123", params=None
) -> MagicMock:
    ctx = MagicMock()
    ctx.name = name
    ctx.id = run_id
    ctx.params = params if params is not None else NoParams()
    return ctx


async def test_strip_serve_removes_trailing_command() -> None:
    assert strip_serve(["python", "-m", "myapp", "serve"]) == ["python", "-m", "myapp"]


async def test_strip_serve_removes_serve_options() -> None:
    """`serve` accepts --host, --port and --reload, they belong to `serve`"""
    command = ["python", "-m", "myapp", "serve", "-p", "8080", "-h", "0.0.0.0"]
    assert strip_serve(command) == ["python", "-m", "myapp"]
    assert strip_serve([*command, "--reload"]) == ["python", "-m", "myapp"]


async def test_strip_serve_console_script() -> None:
    assert strip_serve(["myapp", "serve", "--port", "8080"]) == ["myapp"]


async def test_strip_serve_without_serve_unchanged() -> None:
    assert strip_serve(["python", "-m", "myapp"]) == ["python", "-m", "myapp"]


async def test_strip_serve_empty_command() -> None:
    assert strip_serve([]) == []


async def test_exec_args_carries_name_and_run_id() -> None:
    args = exec_args(make_ctx(name="my_task", run_id="run-99"))
    assert args[:2] == ["exec", "my_task"]
    assert "--log" in args
    assert args[args.index("--run-id") + 1] == "run-99"


async def test_exec_args_reveals_secret_params() -> None:
    """The process executing the task re-validates the params"""
    args = exec_args(make_ctx(params=SecretParams(token=SecretStr("s3cret"))))
    params = json.loads(args[args.index("--params") + 1])
    assert params == {"token": "s3cret"}


async def test_cpu_bound_command_derives_from_entry_point(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sys.orig_argv", ["python", "-m", "myapp", "serve", "-p", "8080"]
    )
    command = cpu_bound_command(make_ctx(name="my_task"))
    assert command[:3] == ["python", "-m", "myapp"]
    assert command[3:5] == ["exec", "my_task"]
