from pathlib import Path

from fluid.db import CrudDB

from .tables1 import meta
from .tables2 import additional_meta

MIGRATIONS_PATH = Path(__file__).parent / "migrations"


def setup_tables(db: CrudDB) -> CrudDB:
    meta(db.metadata)
    additional_meta(db.metadata)
    return db


def get_db() -> CrudDB:
    return setup_tables(CrudDB.from_env(migration_path=MIGRATIONS_PATH))
