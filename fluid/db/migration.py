from __future__ import annotations

from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, cast

import sqlalchemy as sa
from alembic import command as alembic_cmd
from alembic.config import Config
from sqlalchemy.engine import Engine
from sqlalchemy_utils import create_database, database_exists, drop_database

if TYPE_CHECKING:
    from .container import Database


@dataclass
class Migration:
    """A wrapper around Alembic commands to perform database migrations"""

    db: Database
    cfg: Config = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.cfg = create_config(self.db)

    @property
    def metadata(self) -> sa.MetaData:
        return self.db.metadata

    @property
    def sync_engine(self) -> Engine:
        return self.db.sync_engine

    def init(self) -> str:
        dirname = self.cfg.get_main_option("script_location") or ""
        alembic_cmd.init(self.cfg, dirname)
        return self.message()

    def show(self, revision: str) -> str:
        alembic_cmd.show(self.cfg, revision)
        return self.message()

    def history(self) -> str:
        alembic_cmd.history(self.cfg)
        return self.message()

    def revision(
        self,
        message: str,
        autogenerate: bool = False,
        branch_label: str | None = None,
    ) -> str:
        alembic_cmd.revision(
            self.cfg,
            autogenerate=autogenerate,
            message=message,
            branch_label=branch_label,
        )
        return self.message()

    def upgrade(self, revision: str) -> str:
        alembic_cmd.upgrade(self.cfg, revision)
        return self.message()

    def downgrade(self, revision: str) -> str:
        alembic_cmd.downgrade(self.cfg, revision)
        return self.message()

    def current(self, verbose: bool = False) -> str:
        alembic_cmd.current(self.cfg, verbose=verbose)
        return self.message()

    def message(self) -> str:
        msg = cast(StringIO, self.cfg.stdout).getvalue()
        self.cfg.stdout.seek(0)
        self.cfg.stdout.truncate()
        return msg

    def db_exists(self, dbname: str = "") -> bool:
        url = self.sync_engine.url
        if dbname:
            url = url.set(database=dbname)
        return database_exists(url)

    def db_create(self, dbname: str = "") -> bool:
        """Creates a new database if it does not exist"""
        url = self.sync_engine.url
        if dbname:
            url = url.set(database=dbname)
        if database_exists(url):
            return False
        create_database(url)
        return True

    def db_drop(self, dbname: str = "") -> bool:
        url = self.sync_engine.url
        if dbname:
            url = url.set(database=dbname)
        if database_exists(url):
            drop_database(url)
            return True
        return False

    def truncate_all(self) -> None:
        """Drop all tables from :attr:`metadata` in database"""
        with self.sync_engine.begin() as conn:
            conn.execute(sa.text(f'truncate {", ".join(self.metadata.tables)}'))

    def drop_all_schemas(self) -> None:
        """Drop all schema in database"""
        with self.sync_engine.begin() as conn:
            conn.execute(sa.text("DROP SCHEMA IF EXISTS public CASCADE"))
            conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS public"))

    def create_ro_user(
        self,
        username: str,
        password: str,
        role: str = "",
        schema: str = "public",
    ) -> bool:
        """Creates a read-only user"""
        engine = self.sync_engine
        role = role or f"{engine.url.username}_ro"
        database = engine.url.database
        created = True
        with engine.begin() as conn:
            try:
                conn.execute(sa.text(f"CREATE ROLE {role};"))
            except sa.exc.ProgrammingError:
                created = False
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    f"GRANT CONNECT ON DATABASE {database} TO {role};"
                    f"GRANT USAGE ON SCHEMA {schema} TO {role};"
                    f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role};"
                    f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {role};",
                ),
            )
            conn.execute(
                sa.text(
                    f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
                    f"GRANT SELECT ON TABLES TO {role};",
                ),
            )
        with engine.begin() as conn:
            try:
                conn.execute(
                    sa.text(
                        f"CREATE USER {username} WITH PASSWORD '{password}';"
                        f"GRANT {role} TO {username};",
                    ),
                )
            except sa.exc.ProgrammingError:
                created = False
        return created

    def drop_role(
        self,
        role: str,
    ) -> bool:
        """Drop a role"""
        try:
            with self.sync_engine.begin() as conn:
                conn.execute(sa.text(f"DROP OWNED BY {role};"))
            with self.sync_engine.begin() as conn:
                conn.execute(sa.text(f"DROP ROLE IF EXISTS {role};"))
        except sa.exc.ProgrammingError as exc:
            if f'role "{role}" does not exist' not in str(exc):
                raise
            return False
        return True


def create_config(db: "Database") -> Config:
    """Programmatically create Alembic config"""
    cfg = Config(stdout=StringIO())
    cfg.set_main_option("script_location", str(db.migration_path))
    cfg.config_file_name = str(Path(db.migration_path) / "alembic.ini")
    cfg.set_section_option(
        "alembic",
        "sqlalchemy.url",
        db.sync_engine.url.render_as_string(hide_password=False),
    )
    # create empty logging section to avoid raising errors in env.py
    cfg.set_section_option("logging", "path", "")
    cfg.metadata = db.metadata  # type: ignore
    return cfg
