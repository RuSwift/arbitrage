"""crawler_iteration: add inactive_till_timestamp (nullable)

Revision ID: e9f0a1b2c3d4
Revises: d4e5f6a7b8c9
Create Date: 2026-03-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "e9f0a1b2c3d4"
down_revision: Union[str, Sequence[str], None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "crawler_iteration",
        sa.Column("inactive_till_timestamp", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("crawler_iteration", "inactive_till_timestamp")
