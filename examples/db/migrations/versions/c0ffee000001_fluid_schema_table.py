"""fluid schema table

Revision ID: c0ffee000001
Revises: b2ee259b4966
Create Date: 2026-08-30 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c0ffee000001"
down_revision: Union[str, None] = "b2ee259b4966"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.schema.CreateSchema("fluid", if_not_exists=True))
    op.create_table(
        "fluid_tasks",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema="fluid",
    )


def downgrade() -> None:
    op.drop_table("fluid_tasks", schema="fluid")
    op.execute(sa.schema.DropSchema("fluid", cascade=True, if_exists=True))
