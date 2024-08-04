from datetime import date

import sqlalchemy as sa

from fluid.utils.dates import utcnow
from fluid.utils.text import create_uid


def additional_meta(meta=None):
    """Add task related tables"""
    if meta is None:
        meta = sa.MetaData()

    sa.Table(
        "randoms",
        meta,
        sa.Column(
            "id",
            sa.String(32),
            primary_key=True,
            nullable=False,
            default=create_uid,
        ),
        sa.Column("randomdate", sa.Date, nullable=False, default=date.today),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            nullable=False,
            default=utcnow,
        ),
        sa.Column("price", sa.Numeric(precision=100, scale=4), nullable=False),
        sa.Column("tenor", sa.String(3), nullable=False),
        sa.Column("tick", sa.Boolean),
        sa.Column("info", sa.JSON),
        sa.Column("jsonlist", sa.JSON, default=[]),
        sa.Column(
            "task_id", sa.ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False
        ),
    )

    sa.Table(
        "multi_key_unique",
        meta,
        sa.Column("x", sa.Integer, nullable=False),
        sa.Column("y", sa.Integer, nullable=False),
        sa.UniqueConstraint("x", "y"),
    )

    sa.Table(
        "multi_key",
        meta,
        sa.Column("x", sa.JSON),
        sa.Column("y", sa.JSON),
    )

    return meta


def extra(meta):
    sa.Table(
        "extras",
        meta,
        sa.Column(
            "id",
            sa.String(32),
            primary_key=True,
            nullable=False,
            default=create_uid,
        ),
        sa.Column("name", sa.String(64), nullable=False),
    )
