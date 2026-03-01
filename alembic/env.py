"""
Alembic environment configuration.
По образцу https://github.com/RuSwift/garantex
"""
from logging.config import fileConfig
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from alembic import context
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from .env file in project root
env_file_path = project_root / ".env"
if env_file_path.exists():
    load_dotenv(env_file_path, override=False)

# Import settings and Base for metadata (для autogenerate подключайте сюда модели)
from app.settings import Settings
from app.db import Base

config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Get database URL from settings
db_settings = Settings().database
config.set_main_option("sqlalchemy.url", db_settings.url)

# Для autogenerate: при добавлении моделей импортируйте их сюда
from app.db.models import (  # noqa: F401
    BookDepthSnapshot,
    CandleStickSnapshot,
    CrawlerIteration,
    CrawlerJob,
    CurrencyPairSnapshot,
    ServiceConfig,
    Token,
)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode (async)."""
    connectable: AsyncEngine = create_async_engine(
        db_settings.async_url,
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    import asyncio
    asyncio.run(run_migrations_online())
