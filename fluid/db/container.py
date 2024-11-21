from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Self

import sqlalchemy as sa
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from fluid import settings

from .cli import DbGroup
from .migration import Migration


@dataclass
class Database:
    """A container for tables in a database and a manager of asynchronous
    connections to a postgresql database
    """

    dsn: str
    """data source name, aka connection string"""
    echo: bool = settings.DBECHO
    pool_size: int = settings.DBPOOL_MAX_SIZE
    max_overflow: int = settings.DBPOOL_MAX_OVERFLOW
    metadata: sa.MetaData = field(default_factory=sa.MetaData)
    migration_path: str = ""
    app_name: str = settings.APP_NAME
    _engine: AsyncEngine | None = None

    @classmethod
    def from_env(
        cls,
        *,
        dsn: str = settings.DATABASE,
        schema: str | None = settings.DATABASE_SCHEMA,
        **kwargs: Any,
    ) -> Self:
        """Create a new database conatiner from environment variables as defaults"""
        return cls(dsn=dsn, metadata=sa.MetaData(schema=schema), **kwargs)

    @property
    def tables(self) -> dict[str, sa.Table]:
        return self.metadata.tables

    @property
    def engine(self) -> AsyncEngine:
        """The :class:`sqlalchemy.ext.asyncio.AsyncEngine` creating connection
        and transactions"""
        if self._engine is None:
            self._engine = create_async_engine(
                self.dsn,
                echo=self.echo,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                connect_args=dict(server_settings=dict(application_name=self.app_name)),
            )
        return self._engine

    @property
    def sync_engine(self) -> Engine:
        """The :class:`sqlalchemy.engine.Engine` for synchrouns operations"""
        return create_engine(self.dsn.replace("+asyncpg", ""))

    def cli(self, **kwargs: Any) -> DbGroup:
        """Create a new click group for database commands"""
        return DbGroup(self, **kwargs)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection]:
        """Context manager for obtaining an asynchronous connection"""
        async with self.engine.connect() as conn:
            yield conn

    @asynccontextmanager
    async def ensure_connection(
        self,
        conn: AsyncConnection | None = None,
    ) -> AsyncIterator[AsyncConnection]:
        """Context manager for obtaining an asynchronous connection"""
        if conn:
            yield conn
        else:
            async with self.engine.connect() as conn:
                yield conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection]:
        """Context manager for initializing an asynchronous database transaction"""
        async with self.engine.begin() as conn:
            yield conn

    @asynccontextmanager
    async def ensure_transaction(
        self,
        conn: AsyncConnection | None = None,
    ) -> AsyncIterator[AsyncConnection]:
        """Context manager for ensuring we a connection has initialized
        a database transaction"""
        if conn:
            if not conn.in_transaction():
                async with conn.begin():
                    yield conn
            else:
                yield conn
        else:
            async with self.transaction() as conn:
                yield conn

    async def close(self) -> None:
        """Close the asynchronous db engine if opened"""
        if self._engine is not None:
            engine, self._engine = self._engine, None
            await engine.dispose()

    async def ping(self) -> str:
        """Ping the database"""
        # TODO: we need a custom ping query
        async with self.connection() as conn:
            await conn.execute(sa.text("SELECT 1"))
        return "ok"

    def migration(self) -> Migration:
        """Create a new migration manager for this database"""
        return Migration(self)
