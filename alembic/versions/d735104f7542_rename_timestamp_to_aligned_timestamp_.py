"""rename_timestamp_to_aligned_timestamp_add_utc_candle_stick

Revision ID: d735104f7542
Revises: 3edac122b283
Create Date: 2026-03-01 00:22:22.310233

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd735104f7542'
down_revision: Union[str, Sequence[str], None] = '3edac122b283'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Rename timestamp -> aligned_timestamp; add utc to candle_stick_snapshot."""
    op.alter_column(
        "currency_pair_snapshot",
        "timestamp",
        new_column_name="aligned_timestamp",
        existing_type=sa.Float(),
        existing_nullable=False,
    )
    op.alter_column(
        "book_depth_snapshot",
        "timestamp",
        new_column_name="aligned_timestamp",
        existing_type=sa.Float(),
        existing_nullable=False,
    )
    op.alter_column(
        "candle_stick_snapshot",
        "timestamp",
        new_column_name="aligned_timestamp",
        existing_type=sa.Float(),
        existing_nullable=False,
    )
    op.add_column(
        "candle_stick_snapshot",
        sa.Column("utc", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    """Revert: aligned_timestamp -> timestamp; drop utc from candle_stick_snapshot."""
    op.drop_column("candle_stick_snapshot", "utc")
    op.alter_column(
        "candle_stick_snapshot",
        "aligned_timestamp",
        new_column_name="timestamp",
        existing_type=sa.Float(),
        existing_nullable=False,
    )
    op.alter_column(
        "book_depth_snapshot",
        "aligned_timestamp",
        new_column_name="timestamp",
        existing_type=sa.Float(),
        existing_nullable=False,
    )
    op.alter_column(
        "currency_pair_snapshot",
        "aligned_timestamp",
        new_column_name="timestamp",
        existing_type=sa.Float(),
        existing_nullable=False,
    )
