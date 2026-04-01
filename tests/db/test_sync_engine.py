"""Tests for Database.sync_engine URL construction — no live DB required."""

from fluid.db.container import Database


def test_sync_engine_strips_asyncpg_driver() -> None:
    db = Database(dsn="postgresql+asyncpg://user:pass@localhost/mydb")
    engine = db.sync_engine
    assert engine.url.drivername == "postgresql"


def test_sync_engine_converts_ssl_to_sslmode() -> None:
    db = Database(dsn="postgresql+asyncpg://user:pass@localhost/mydb?ssl=require")
    engine = db.sync_engine
    assert engine.url.query.get("sslmode") == "require"
    assert "ssl" not in engine.url.query


def test_sync_engine_no_ssl_param_unchanged() -> None:
    db = Database(dsn="postgresql+asyncpg://user:pass@localhost/mydb")
    engine = db.sync_engine
    assert "sslmode" not in engine.url.query
    assert "ssl" not in engine.url.query


def test_sync_engine_preserves_other_query_params() -> None:
    db = Database(
        dsn="postgresql+asyncpg://user:pass@localhost/mydb?ssl=verify-full&connect_timeout=10"
    )
    engine = db.sync_engine
    assert engine.url.query.get("sslmode") == "verify-full"
    assert engine.url.query.get("connect_timeout") == "10"
    assert "ssl" not in engine.url.query
