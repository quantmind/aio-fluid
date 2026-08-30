from pathlib import Path

import pytest
import sqlalchemy as sa
from click.testing import CliRunner

from examples.db.cli import cli
from fluid.db import CrudDB


@pytest.fixture
def mig_id():
    return "c0ffee000001"


@pytest.fixture
def mig_name():
    return "fluid schema table"


def test_cli():
    assert isinstance(cli.db, CrudDB)


async def test_migrations_init_wires_metadata(tmp_path: Path, db: CrudDB):
    """``init`` must generate an env.py wired to the database metadata so that
    ``--autogenerate`` works without manual editing."""
    new_db = CrudDB.from_env(
        dsn=db.engine.url.render_as_string(hide_password=False),
        migration_path=tmp_path / "migrations",
    )
    sa.Table("widget", new_db.metadata, sa.Column("id", sa.Integer, primary_key=True))
    new_db.migration().init()

    env = (tmp_path / "migrations" / "env.py").read_text()
    assert 'target_metadata = getattr(config, "metadata", None)' in env
    assert "target_metadata = None" not in env


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


def test_schemas_lists_non_system_schemas(db: CrudDB):
    mig = db.migration()
    with db.sync_engine.begin() as conn:
        conn.execute(sa.schema.CreateSchema("app_schema", if_not_exists=True))
    try:
        schemas = mig.schemas()
    finally:
        with db.sync_engine.begin() as conn:
            conn.execute(
                sa.schema.DropSchema("app_schema", cascade=True, if_exists=True)
            )
    assert "app_schema" in schemas
    assert "public" in schemas
    assert not any(s.startswith("pg_") for s in schemas)
    assert "information_schema" not in schemas


def test_drop_all_schemas_defaults_to_public(db: CrudDB):
    mig = db.migration()
    with db.sync_engine.begin() as conn:
        conn.execute(sa.schema.CreateSchema("app_schema", if_not_exists=True))
    mig.drop_all_schemas()
    schemas = mig.schemas()
    assert "public" in schemas
    assert "app_schema" in schemas


def test_drop_all_schemas_explicit_list(db: CrudDB):
    mig = db.migration()
    with db.sync_engine.begin() as conn:
        conn.execute(sa.schema.CreateSchema("app_schema", if_not_exists=True))
    try:
        mig.drop_all_schemas(["app_schema"])
        schemas = mig.schemas()
    finally:
        with db.sync_engine.begin() as conn:
            conn.execute(
                sa.schema.DropSchema("app_schema", cascade=True, if_exists=True)
            )
    assert "app_schema" not in schemas
    assert "public" in schemas


def test_table_on_separate_schema(db: CrudDB):
    """A table registered on its own schema is created and queryable there."""
    table = db.tables["fluid.fluid_tasks"]
    with db.sync_engine.begin() as conn:
        conn.execute(sa.insert(table), [dict(name="first")])
        rows = conn.execute(sa.select(table)).fetchall()
    assert rows
    assert rows[0].name == "first"
    mig = db.migration()
    assert "fluid" in mig.schemas()


def test_drop_all_schemas_with_fluid(db: CrudDB):
    """`drop_all_schemas` drops the extra schema when passed explicitly."""
    db.tables["fluid.fluid_tasks"]
    mig = db.migration()
    mig.drop_all_schemas(["fluid", "public"])
    assert "fluid" not in mig.schemas()
