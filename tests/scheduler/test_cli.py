import pytest
from click.testing import CliRunner
from redis import Redis

from examples.cli import task_manager_cli
from examples.tasks import Palette, PaletteParams
from fluid import settings
from fluid.scheduler.cli import TaskManagerCLI


@pytest.fixture(scope="module")
def redis() -> Redis:
    return Redis.from_url(settings.BROKER_URL)


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
