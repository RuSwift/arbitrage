"""
Настройки приложения с использованием pydantic_settings.
По образцу https://github.com/RuSwift/garantex
"""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr
from typing import Optional

# .env в корне проекта (родитель каталога app)
_ROOT_DIR = Path(__file__).resolve().parent.parent.parent
_ENV_FILE = _ROOT_DIR / ".env"


class DatabaseSettings(BaseSettings):
    """Настройки подключения к PostgreSQL"""

    model_config = SettingsConfigDict(
        env_prefix="DB_",
        case_sensitive=False,
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(
        default="localhost",
        description="Хост базы данных PostgreSQL",
    )

    port: int = Field(
        default=5432,
        description="Порт базы данных PostgreSQL",
    )

    user: str = Field(
        default="postgres",
        description="Имя пользователя базы данных",
    )

    password: SecretStr = Field(
        default=SecretStr(""),
        description="Пароль базы данных",
    )

    database: str = Field(
        default="arbitrage",
        description="Имя базы данных",
    )

    pool_size: int = Field(
        default=5,
        description="Размер пула соединений",
    )

    max_overflow: int = Field(
        default=10,
        description="Максимальное количество переполнений пула",
    )

    pool_timeout: int = Field(
        default=30,
        description="Таймаут ожидания соединения из пула (секунды)",
    )

    echo: bool = Field(
        default=False,
        description="Логировать SQL запросы",
    )

    @property
    def url(self) -> str:
        """Возвращает URL подключения к базе данных"""
        password_value = self.password.get_secret_value() if self.password else ""
        return f"postgresql://{self.user}:{password_value}@{self.host}:{self.port}/{self.database}"

    @property
    def async_url(self) -> str:
        """Возвращает async URL подключения к базе данных"""
        password_value = self.password.get_secret_value() if self.password else ""
        return f"postgresql+asyncpg://{self.user}:{password_value}@{self.host}:{self.port}/{self.database}"


class RedisSettings(BaseSettings):
    """Настройки подключения к Redis"""

    model_config = SettingsConfigDict(
        env_prefix="REDIS_",
        case_sensitive=False,
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(
        default="localhost",
        description="Хост Redis",
    )

    port: int = Field(
        default=6378,
        description="Порт Redis",
    )

    password: Optional[SecretStr] = Field(
        default=None,
        description="Пароль Redis (опционально)",
    )

    db: int = Field(
        default=0,
        description="Номер базы данных Redis",
    )

    @property
    def url(self) -> str:
        """Возвращает URL подключения к Redis"""
        password_value = self.password.get_secret_value() if self.password else ""
        if password_value:
            return f"redis://:{password_value}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class CoinMarketCapSettings(BaseSettings):
    """Настройки CoinMarketCap API. API ключ обязателен (env: COINMARKETCAP_API_KEY)."""

    model_config = SettingsConfigDict(
        env_prefix="COINMARKETCAP_",
        case_sensitive=False,
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_key: SecretStr = Field(
        default=SecretStr(""),
        description="API ключ CoinMarketCap (env: COINMARKETCAP_API_KEY)",
    )


class Settings(BaseSettings):
    """Основные настройки приложения"""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = Field(
        default="Arbitrage",
        description="Название приложения",
    )

    app_version: str = Field(
        default="1.0.0",
        description="Версия приложения",
    )

    debug: bool = Field(
        default=False,
        description="Режим отладки",
    )

    secret: SecretStr = Field(
        default=SecretStr("default-secret-key-change-in-production"),
        description="Secret key для шифрования и подписи",
    )

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    coinmarketcap: CoinMarketCapSettings = Field(default_factory=CoinMarketCapSettings)


__all__ = [
    "Settings",
    "DatabaseSettings",
    "RedisSettings",
    "CoinMarketCapSettings",
]
