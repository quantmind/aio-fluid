from examples.db.cli import cli
from fluid.db import CrudDB


def test_cli():
    assert isinstance(cli.db, CrudDB)
