from pathlib import Path
from typing import AsyncIterator

import pytest
import sqlalchemy as sa
from click.testing import CliRunner

from fluid.db import CrudDB
from fluid.db.cli import DbGroup

pytestmark = pytest.mark.asyncio(loop_scope="module")

DB_NAME = "fluid_cli_migrations"


@pytest.fixture
async def new_db(tmp_path: Path, db: CrudDB) -> AsyncIterator[CrudDB]:
    mig = db.migration()
    mig.db_drop(DB_NAME)
    assert mig.db_create(DB_NAME)
    new_db = CrudDB.from_env(
        dsn=db.engine.url.render_as_string(hide_password=False),
        db_name=DB_NAME,
        migration_path=tmp_path / "migrations",
    )
    sa.Table(
        "widget",
        new_db.metadata,
        sa.Column("id", sa.Integer, primary_key=True),
    )
    try:
        yield new_db
    finally:
        await new_db.close()
        mig.db_drop(DB_NAME)


def invoke(cli: DbGroup, *args: str) -> str:
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == 0, result.output
    return result.output


def table_names(db: CrudDB) -> set[str]:
    return set(sa.inspect(db.sync_engine).get_table_names())


async def test_migration_workflow(tmp_path: Path, new_db: CrudDB) -> None:
    cli = DbGroup(new_db)
    versions = tmp_path / "migrations" / "versions"

    invoke(cli, "init")
    assert (tmp_path / "migrations" / "env.py").exists()

    invoke(cli, "migrate", "-m", "add widget")
    (script,) = versions.glob("*.py")
    assert "create_table('widget'" in script.read_text().replace('"', "'")

    output = invoke(cli, "upgrade")
    assert "upgraded successfully to heads" in output
    assert "widget" in table_names(new_db)

    invoke(cli, "revision", "-m", "empty revision")
    assert len(list(versions.glob("*.py"))) == 2

    output = invoke(cli, "downgrade", "base")
    assert "downgraded successfully to base" in output
    assert "widget" not in table_names(new_db)
