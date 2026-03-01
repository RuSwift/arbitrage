"""crawler_job: add kind (spot | perpetual)

Revision ID: d4e5f6a7b8c9
Revises: c8e9d0f1a2b3
Create Date: 2026-03-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, Sequence[str], None] = "c8e9d0f1a2b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "crawler_job",
        sa.Column("kind", sa.String(), nullable=False, server_default=sa.text("'perpetual'")),
    )


def downgrade() -> None:
    op.drop_column("crawler_job", "kind")
