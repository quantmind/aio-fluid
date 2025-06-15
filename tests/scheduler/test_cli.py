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
    result = runner.invoke(task_manager_cli, ["db"])
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
    result = runner.invoke(task_manager_cli, ["exec"])
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
