from click.testing import CliRunner

from examples.db.cli import cli
from fluid.db import CrudDB


def test_cli():
    assert isinstance(cli.db, CrudDB)


def test_create_drop_db():
    runner = CliRunner()
    result = runner.invoke(cli, ["create", "test_db_abc"])
    assert result.exit_code == 0
    assert "database 'test_db_abc' created" in result.output
    result = runner.invoke(cli, ["create", "test_db_abc"])
    assert result.exit_code == 0
    assert "database 'test_db_abc' already available" in result.output
    result = runner.invoke(cli, ["drop", "test_db_abc", "-y"])
    assert result.exit_code == 0
    assert "database 'test_db_abc' dropped" in result.output
    result = runner.invoke(cli, ["drop", "test_db_abc", "-y"])
    assert result.exit_code == 0
    assert "database 'test_db_abc' not found" in result.output
