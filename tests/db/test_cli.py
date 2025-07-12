import pytest
from click.testing import CliRunner

from examples.db.cli import cli
from fluid.db import CrudDB


@pytest.fixture
def mig_id():
    return "b2ee259b4966"


@pytest.fixture
def mig_name():
    return "initial"


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


def test_migrations_show(mig_id, mig_name):
    runner = CliRunner()
    result = runner.invoke(cli, ["show"])
    assert result.exit_code == 0
    assert mig_id in result.output
    assert mig_name in result.output


def test_migrations_history(mig_id, mig_name):
    runner = CliRunner()
    result = runner.invoke(cli, ["history"])
    assert result.exit_code == 0
    assert mig_id in result.output
    assert mig_name in result.output


def test_migrations_current(mig_id):
    runner = CliRunner()
    result = runner.invoke(cli, ["current"])
    assert result.exit_code == 0
    assert mig_id in result.output
