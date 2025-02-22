from examples.db import get_db
from fluid.db.cli import DbGroup

cli = DbGroup(db=get_db())
