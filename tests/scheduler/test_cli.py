from click.testing import CliRunner

from examples.cli import task_manager_cli


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
