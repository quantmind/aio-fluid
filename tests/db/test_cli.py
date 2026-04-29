import pytest
import sqlalchemy as sa
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


def test_create_drop_db(db: CrudDB):
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


def test_migrations_show(mig_id: str, mig_name: str, db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["show"])
    assert result.exit_code == 0
    assert mig_id in result.output
    assert mig_name in result.output


def test_migrations_history(mig_id: str, mig_name: str, db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["history"])
    assert result.exit_code == 0
    assert mig_id in result.output
    assert mig_name in result.output


def test_migrations_current(mig_id: str, db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["current"])
    assert result.exit_code == 0
    assert mig_id in result.output


def test_migrations_databases(db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["databases"])
    assert result.exit_code == 0
    assert db.engine.url.database
    assert db.engine.url.database in result.output


def test_tables(db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["tables"])
    assert result.exit_code == 0
    assert "tasks" in result.output


def test_truncate(db: CrudDB):
    mig = db.migration()
    mig.truncate("tasks", cascade=True)


def test_truncate_all(db: CrudDB):
    mig = db.migration()
    mig.truncate_all()


def test_create_ro_user_and_drop_role(db: CrudDB):
    mig = db.migration()
    mig.drop_role("test_ro_role")
    with db.sync_engine.begin() as conn:
        conn.execute(sa.text("DROP USER IF EXISTS test_ro_user"))
    created = mig.create_ro_user("test_ro_user", "secret", role="test_ro_role")
    assert created is True
    created = mig.create_ro_user("test_ro_user", "secret", role="test_ro_role")
    assert created is False
    dropped = mig.drop_role("test_ro_role")
    assert dropped is True
    dropped = mig.drop_role("test_ro_role")
    assert dropped is False


def test_create_ro_user_cli(db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["create-ro-user", "test_cli_ro_user", "secret", "--role", "test_cli_ro_role"],
    )
    assert result.exit_code == 0
    mig = db.migration()
    mig.drop_role("test_cli_ro_role")


def test_drop_role_cli(db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["drop-role", "nonexistent_role_xyz"])
    assert result.exit_code == 0
    assert "not found" in result.output


def test_delete_rows_unknown_table(db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["delete-rows", "no_such_table"])
    assert result.exit_code != 0


def test_delete_rows_dry(db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["delete-rows", "tasks", "--dry"])
    assert result.exit_code == 0
    assert "dry mode" in result.output


def test_delete_rows(db: CrudDB):
    runner = CliRunner()
    result = runner.invoke(cli, ["delete-rows", "tasks"])
    assert result.exit_code == 0
    assert "removing" in result.output
