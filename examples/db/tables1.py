import enum

import sqlalchemy as sa

from fluid.utils.dates import utcnow
from fluid.utils.text import create_uid


class TaskType(enum.Enum):
    todo = 0
    issue = 1


def meta(meta: sa.MetaData) -> None:
    """Add task related tables"""

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
        sa.Column("created", sa.DateTime(timezone=True), default=utcnow),
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
