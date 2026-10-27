import asyncio
import logging
from typing import Any, Iterator

import click
import pytest
import uvicorn
from click.testing import CliRunner
from fastapi import FastAPI
from redis import Redis

from examples import tasks
from examples.cli import task_manager_cli
from examples.tasks import Palette, PaletteParams
from fluid import settings
from fluid.scheduler.cli import DEFAULT_COMMANDS, TaskManagerCLI, ctx_app
from fluid.scheduler.errors import TaskRunError
from tests.scheduler.tasks import redis_broker


@pytest.fixture(scope="module")
def redis() -> Redis:
    return Redis.from_url(settings.BROKER_URL)


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Commands which configure logging must not leak it to other tests"""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


@click.command("app-title")
@click.pass_context
def app_title(ctx: click.Context) -> None:
    """Print the title of the task manager app"""
    click.echo(ctx_app(ctx).title)


def test_cli():
    assert task_manager_cli.task_manager_app
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["--help"])
    assert result.exit_code == 0


def test_cli_db():
    assert task_manager_cli.task_manager_app
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["db", "--help"])
    assert result.exit_code == 0
    result = runner.invoke(task_manager_cli, ["db", "dsn"])
    assert result.exit_code == 0


def test_cli_ls():
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["ls"])
    assert result.exit_code == 0
    assert result.output
    assert "ping" in result.output
    assert "dummy" in result.output


def test_cli_ls_tags():
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["ls", "--tags", "skip_db"])
    assert result.exit_code == 0
    assert "ping" in result.output
    assert "dummy" not in result.output
    result = runner.invoke(task_manager_cli, ["ls", "-t", "slow", "-t", "fast"])
    assert result.exit_code == 0
    assert "dummy" in result.output
    assert "fast" in result.output
    assert "ping" not in result.output


def test_cli_exec_empty():
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "--help"])
    assert result.exit_code == 0
    assert result.output.startswith(
        "\n".join(
            (
                "Usage: root exec [OPTIONS] COMMAND [ARGS]...",
                "",
                "  Execute a registered task",
            )
        )
    )


def test_cli_exec_ping():
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "ping"])
    assert result.exit_code == 0
    assert result.output


def test_cli_exec_aborted():
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "dummy", "--abort"])
    assert result.exit_code == 0
    assert "aborted" in result.output


def test_cli_exec_params_json_overrides_model_defaults():
    """JSON --params must take priority over BaseModel field defaults.

    The `fast` task uses a Sleep model with error=False as default.
    Passing --params '{"error": true}' must cause the task to fail.
    Before the fix, model_dump() ran after json.loads() and overwrote
    the JSON value with the model default, so the task would succeed.
    """
    runner = CliRunner()
    result = runner.invoke(
        task_manager_cli, ["exec", "fast", "--params", '{"error": true}']
    )
    # The task raises RuntimeError when error=True, so exit_code is 1.
    # If the bug were present, the model default error=False would overwrite
    # the JSON value and the task would succeed (exit_code=0).
    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)


def test_cli_exec_cpu_bound_runs_in_process():
    """`exec` runs a cpu bound task rather than spawning one for it.

    It is the command a cpu bound task is executed by, so spawning would
    recurse. The error the task raises reaches the caller as itself; a spawned
    process would report the exit code as a TaskRunError instead.
    """
    runner = CliRunner()
    result = runner.invoke(
        task_manager_cli, ["exec", "cpu_bound", "--params", '{"error": true}']
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert not isinstance(result.exception, TaskRunError)
    assert str(result.exception) == "deliberate cpu bound error"


def test_cli_enable():
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["enable", "ping", "--disable"])
    assert result.exit_code == 0
    assert not result.output
    result = runner.invoke(task_manager_cli, ["enable", "ping"])
    assert result.exit_code == 0
    assert not result.output


def test_cli_enable_failure():
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["enable", "vdvdfvsdvdf"])
    assert result.exit_code == 1
    assert result.output == "Error: Task vdvdfvsdvdf not found\n"


def test_cli_exec_colorize_with_default():
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "colorize"])
    assert result.exit_code == 0


def test_cli_exec_colorize_with_str_enum():
    """Invoking a task whose params model contains a StrEnum field must succeed.

    pydanclick falls back to _create_custom_type for unknown types, which calls
    validate_json() on the raw CLI string. "green" is not valid JSON, so without
    a fix the command exits with an error instead of running the task.
    """
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "colorize", "--color", "green"])
    assert result.exit_code == 0


def test_cli_exec_colorize_params_from_model_dump_json(redis: Redis):
    """--params built from model_dump_json() must pass the correct
    StrEnum value to the task."""
    run_id = "test-colorize-blue"
    params = PaletteParams(color=Palette.BLUE).model_dump_json()
    runner = CliRunner()
    result = runner.invoke(
        task_manager_cli,
        ["exec", "colorize", "--run-id", run_id, "--params", params],
    )
    assert result.exit_code == 0
    value = redis.get(run_id)
    assert value is not None and value.decode() == Palette.BLUE


def test_cli_exec_colorize_invalid_choice():
    """An invalid enum value must be rejected by click.Choice
    before the task runs."""
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "colorize", "--color", "purple"])
    assert result.exit_code == 2
    assert "Invalid value for '--color'" in result.output


def test_cli_exec_colorize_params_overrides_color_flag(redis: Redis):
    """--params must take priority over --color for StrEnum fields."""
    run_id = "test-colorize-override"
    runner = CliRunner()
    result = runner.invoke(
        task_manager_cli,
        [
            "exec",
            "colorize",
            "--run-id",
            run_id,
            "--color",
            "red",
            "--params",
            '{"color": "blue"}',
        ],
    )
    assert result.exit_code == 0
    value = redis.get(run_id)
    assert value is not None and value.decode() == Palette.BLUE


def test_cli_exec_colorize_optional_no_color():
    """Optional StrEnum field with no value passed must succeed."""
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "colorize_optional"])
    assert result.exit_code == 0


def test_cli_exec_colorize_optional_with_color(redis: Redis):
    """Optional StrEnum field must accept a valid value."""
    run_id = "test-colorize-optional-green"
    runner = CliRunner()
    result = runner.invoke(
        task_manager_cli,
        ["exec", "colorize_optional", "--run-id", run_id, "--color", "green"],
    )
    assert result.exit_code == 0
    value = redis.get(run_id)
    assert value is not None and value.decode() == Palette.GREEN


def test_cli_exec_colorize_required_missing_color():
    """Required StrEnum field with no value must fail
    with a Click missing option error."""
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "colorize_required"])
    assert result.exit_code == 2
    assert "Missing option '--color'" in result.output


def test_cli_exec_colorize_required_with_color(redis: Redis):
    """Required StrEnum field must succeed when the value is provided."""
    run_id = "test-colorize-required-red"
    runner = CliRunner()
    result = runner.invoke(
        task_manager_cli,
        ["exec", "colorize_required", "--run-id", run_id, "--color", "red"],
    )
    assert result.exit_code == 0
    value = redis.get(run_id)
    assert value is not None and value.decode() == Palette.RED


def test_cli_app_and_task_manager():
    """TaskManagerCLI.get_task_manager_app() and .get_task_manager() must return
    the correct app and TaskManager instance."""
    cli = TaskManagerCLI(task_manager_app=task_manager_cli.get_task_manager_app)
    task_manager = cli.get_task_manager()
    assert task_manager


def test_cli_exec_plain_values():
    """Numbers and datetimes are passed as plain strings, not JSON."""
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "add", "--a", "5", "--b", "3"])
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        task_manager_cli, ["exec", "datetime_task", "--dt", "2026-01-01T00:00:00"]
    )
    assert result.exit_code == 0, result.output


async def test_cli_app_instance() -> None:
    """A FastAPI app instance is used as it is"""
    app = tasks.task_app()
    cli = TaskManagerCLI(task_manager_app=app)
    assert cli.get_task_manager_app() is app
    assert cli.get_task_manager() is app.state.task_manager


async def test_cli_app_without_task_manager() -> None:
    cli = TaskManagerCLI(task_manager_app=FastAPI())
    with pytest.raises(RuntimeError, match="task_manager"):
        cli.get_task_manager()


async def test_cli_ctx_app() -> None:
    cli = TaskManagerCLI(
        "examples.tasks:task_app", commands=(*DEFAULT_COMMANDS, app_title)
    )
    result = CliRunner().invoke(cli, ["app-title"])
    assert result.exit_code == 0, result.output
    assert result.output == "Task Manager API\n"


async def test_cli_exec_unknown_task() -> None:
    result = CliRunner().invoke(task_manager_cli, ["exec", "vdvdfvsdvdf"])
    assert result.exit_code == 1
    assert "Error: Task vdvdfvsdvdf not found" in result.output


async def test_cli_exec_with_log(restore_logging: None) -> None:
    result = await asyncio.to_thread(
        CliRunner().invoke, task_manager_cli, ["exec", "ping", "--log"]
    )
    assert result.exit_code == 0, result.output
    assert "Task Run" in result.output


async def test_cli_exec_in_cpu_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """A cpu bound process runs the task and prints nothing"""
    monkeypatch.setenv("TASK_MANAGER_SPAWN", "true")
    result = await asyncio.to_thread(
        CliRunner().invoke, task_manager_cli, ["exec", "ping"]
    )
    assert result.exit_code == 0, result.output
    assert result.output == ""


async def test_cli_serve(
    monkeypatch: pytest.MonkeyPatch, restore_logging: None
) -> None:
    calls: list[tuple[Any, dict]] = []

    def fake_run(app: Any, **kwargs: Any) -> None:
        calls.append((app, kwargs))

    monkeypatch.setattr(uvicorn, "run", fake_run)
    result = CliRunner().invoke(
        task_manager_cli, ["serve", "--port", "9999", "--host", "127.0.0.1"]
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    app, kwargs = calls[0]
    assert app == task_manager_cli.task_manager_app
    assert kwargs["port"] == 9999
    assert kwargs["host"] == "127.0.0.1"
    assert kwargs["reload"] is False
    assert kwargs["log_config"]


def test_cli_exec_params_from_broker(redis: Redis) -> None:
    """A cpu bound process reads its params from the broker, once"""
    broker = redis_broker(task_manager_cli.get_task_manager())
    key = broker.task_params_key("from-broker-1")
    redis.set(key, '{"error": true}', ex=60)
    runner = CliRunner()
    result = runner.invoke(
        task_manager_cli,
        ["exec", "fast", "--run-id", "from-broker-1", "--params-from-broker"],
    )
    # the stored params make the task fail, the model default would not
    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert redis.get(key) is None


def test_cli_exec_params_from_broker_missing() -> None:
    runner = CliRunner()
    result = runner.invoke(
        task_manager_cli,
        ["exec", "fast", "--run-id", "not-stored", "--params-from-broker"],
    )
    assert result.exit_code == 1
    assert "no params stored for task run 'not-stored'" in str(result.exception)


def test_cli_exec_params_from_broker_requires_run_id() -> None:
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["exec", "fast", "--params-from-broker"])
    assert result.exit_code == 2
    assert "--params-from-broker requires --run-id" in result.output
