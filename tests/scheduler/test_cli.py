from click.testing import CliRunner

from examples.cli import task_manager_cli


def test_cli():
    assert task_manager_cli.task_manager_app
    runner = CliRunner()
    result = runner.invoke(task_manager_cli)
    assert result.exit_code == 0


def test_cli_db():
    assert task_manager_cli.task_manager_app
    runner = CliRunner()
    result = runner.invoke(task_manager_cli, ["db"])
    assert result.exit_code == 0
    result = runner.invoke(task_manager_cli, ["db", "dsn"])
    assert result.exit_code == 0
