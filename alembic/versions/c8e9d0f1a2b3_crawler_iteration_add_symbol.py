"""crawler_iteration: add symbol (nullable)

Revision ID: c8e9d0f1a2b3
Revises: a1b2c3d4e5f6
Create Date: 2026-03-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "c8e9d0f1a2b3"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "crawler_iteration",
        sa.Column("symbol", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("crawler_iteration", "symbol")
