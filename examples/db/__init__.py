from fluid.db import CrudDB

from .tables1 import meta
from .tables2 import additional_meta


def setup_tables(db: CrudDB) -> CrudDB:
    meta(db.metadata)
    additional_meta(db.metadata)
    return db
