import enum

import sqlalchemy as sa

from fluid.utils.text import create_uid


class TaskType(enum.Enum):
    todo = 0
    issue = 1


def meta(meta=None):
    """Add task related tables"""
    if meta is None:
        meta = sa.MetaData()

    sa.Table(
        "tasks",
        meta,
        sa.Column(
            "id", sa.String(32), primary_key=True, doc="Unique ID", default=create_uid
        ),
        sa.Column(
            "title",
            sa.String(64),
            nullable=False,
            info=dict(min_length=3),
        ),
        sa.Column("done", sa.DateTime(timezone=True)),
        sa.Column("severity", sa.Integer),
        sa.Column("created_by", sa.String, default="", nullable=False),
        sa.Column("type", sa.Enum(TaskType)),
        sa.Column("unique_title", sa.String, unique=True),
        sa.Column("story_points", sa.Numeric),
        sa.Column("random", sa.String(64)),
        sa.Column(
            "subtitle",
            sa.String(64),
            nullable=False,
            default="",
        ),
    )

    sa.Table(
        "series",
        meta,
        sa.Column("date", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("group", sa.String(32), nullable=False, index=True, default=""),
        sa.Column("value", sa.Numeric(precision=20, scale=8)),
        sa.UniqueConstraint("date", "group"),
    )

    return meta
