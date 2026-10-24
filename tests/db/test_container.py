import pytest
import sqlalchemy as sa

from fluid.db import CrudDB

pytestmark = pytest.mark.asyncio(loop_scope="module")


def dsn(db: CrudDB) -> str:
    return db.engine.url.render_as_string(hide_password=False)


async def test_from_env_db_name(db: CrudDB) -> None:
    other = CrudDB.from_env(dsn=dsn(db), db_name="another_db")
    assert other.url.database == "another_db"
    assert other.url.password == db.url.password
    assert other.url.host == db.url.host


async def test_ping_and_close(db: CrudDB) -> None:
    other = CrudDB.from_env(dsn=dsn(db))
    assert await other.ping() == "ok"
    engine = other.engine
    await other.close()
    assert other._engine is None
    # a closed container opens a new engine on demand
    assert other.engine is not engine
    await other.close()
    # closing twice is a no-op
    await other.close()


async def test_ensure_connection_reuses_connection(db: CrudDB) -> None:
    async with db.connection() as conn:
        async with db.ensure_connection(conn) as same:
            assert same is conn


async def test_ensure_transaction_begins_on_idle_connection(db: CrudDB) -> None:
    async with db.connection() as conn:
        assert not conn.in_transaction()
        async with db.ensure_transaction(conn) as same:
            assert same is conn
            assert conn.in_transaction()
            await conn.execute(sa.text("SELECT 1"))
        assert not conn.in_transaction()


async def test_ensure_transaction_commits(db: CrudDB) -> None:
    table = db.tables["tasks"]
    async with db.connection() as conn:
        async with db.ensure_transaction(conn) as conn:
            await db.db_insert(table, dict(title="committed"), conn=conn)
    assert await db.db_count(table, dict(title="committed")) == 1
