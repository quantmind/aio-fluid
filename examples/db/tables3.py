import sqlalchemy as sa

from fluid.utils.dates import utcnow
from fluid.utils.text import create_uid


def fluid_meta(meta: sa.MetaData) -> None:
    """Add a table living on a separate ``fluid`` schema.

    The ``schema`` argument on the table overrides the metadata default, so the
    table is registered on the shared metadata but lives in ``fluid``.
    """
    sa.Table(
        "fluid_tasks",
        meta,
        sa.Column(
            "id", sa.String(32), primary_key=True, doc="Unique ID", default=create_uid
        ),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), default=utcnow),
        schema="fluid",
    )
