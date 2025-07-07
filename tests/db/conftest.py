import pytest

from examples.db import get_db
from fluid.db import CrudDB


@pytest.fixture(scope="module")
def db() -> CrudDB:
    db = get_db()
    mig = db.migration()
    if not mig.db_create():
        mig.drop_all_schemas()
    mig.upgrade("heads")
    return db
