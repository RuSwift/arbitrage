"""token: created_at default and backfill, updated_at backfill

Revision ID: f7b3c2d1e0a6
Revises: e8f2a1b3c4d5
Create Date: 2026-03-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "f7b3c2d1e0a6"
down_revision: Union[str, Sequence[str], None] = "e8f2a1b3c4d5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("UPDATE token SET created_at = now() WHERE created_at IS NULL")
    op.execute("UPDATE token SET updated_at = now() WHERE updated_at IS NULL")
    op.alter_column(
        "token",
        "created_at",
        existing_type=sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "token",
        "created_at",
        existing_type=sa.DateTime(timezone=True),
        server_default=None,
        nullable=True,
    )
