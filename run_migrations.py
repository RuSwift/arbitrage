"""
Скрипт для запуска миграций Alembic.
По образцу https://github.com/RuSwift/garantex/blob/main/run_migrations.py

Использование (из корня проекта, с venv):
  .venv/bin/python run_migrations.py [команда] [аргументы]
  # или: poetry run python run_migrations.py upgrade
"""
import sys
from pathlib import Path

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent))

from alembic.config import Config
from alembic import command

from app.settings import Settings


def run_migrations(target: str = "head") -> None:
    """Применить миграции Alembic."""
    try:
        db_settings = Settings().database
        print(f"Подключение к БД: {db_settings.host}:{db_settings.port}/{db_settings.database}")

        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", db_settings.url)

        print(f"Применение миграций до версии: {target}")
        command.upgrade(alembic_cfg, target)
        print("✓ Миграции успешно применены!")

    except Exception as e:
        print(f"✗ Ошибка при применении миграций: {e}")
        sys.exit(1)


def downgrade_migrations(revision: str = "-1") -> None:
    """Откатить миграции Alembic."""
    try:
        db_settings = Settings().database
        print(f"Подключение к БД: {db_settings.host}:{db_settings.port}/{db_settings.database}")

        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", db_settings.url)

        print(f"Откат миграций: {revision}")
        command.downgrade(alembic_cfg, revision)
        print("✓ Миграции успешно откачены!")

    except Exception as e:
        print(f"✗ Ошибка при откате миграций: {e}")
        sys.exit(1)


def show_current() -> None:
    """Показать текущую версию миграций."""
    try:
        db_settings = Settings().database
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", db_settings.url)

        command.current(alembic_cfg)

    except Exception as e:
        print(f"✗ Ошибка: {e}")
        sys.exit(1)


def show_history() -> None:
    """Показать историю миграций."""
    try:
        db_settings = Settings().database
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", db_settings.url)

        command.history(alembic_cfg)

    except Exception as e:
        print(f"✗ Ошибка: {e}")
        sys.exit(1)


def create_revision(message: str, *, autogenerate: bool = False) -> None:
    """Создать новую миграцию."""
    try:
        db_settings = Settings().database
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", db_settings.url)

        if autogenerate:
            print(f"Создание автоматической миграции: {message}")
            command.revision(alembic_cfg, message=message, autogenerate=True)
        else:
            print(f"Создание пустой миграции: {message}")
            command.revision(alembic_cfg, message=message)

        print("✓ Миграция успешно создана!")

    except Exception as e:
        print(f"✗ Ошибка при создании миграции: {e}")
        sys.exit(1)


def main() -> None:
    """Главная функция."""
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python run_migrations.py upgrade [revision]     - Применить миграции (по умолчанию: head)")
        print("  python run_migrations.py downgrade [revision]   - Откатить миграции (по умолчанию: -1)")
        print("  python run_migrations.py current                - Показать текущую версию")
        print("  python run_migrations.py history                - Показать историю миграций")
        print("  python run_migrations.py create <message>       - Создать новую миграцию")
        print("  python run_migrations.py autogenerate <message> - Создать автоматическую миграцию")
        sys.exit(0)

    cmd = sys.argv[1].lower()

    if cmd == "upgrade":
        target = sys.argv[2] if len(sys.argv) > 2 else "head"
        run_migrations(target)
    elif cmd == "downgrade":
        revision = sys.argv[2] if len(sys.argv) > 2 else "-1"
        downgrade_migrations(revision)
    elif cmd == "current":
        show_current()
    elif cmd == "history":
        show_history()
    elif cmd == "create":
        if len(sys.argv) < 3:
            print("✗ Укажите сообщение для миграции: python run_migrations.py create 'описание'")
            sys.exit(1)
        create_revision(sys.argv[2], autogenerate=False)
    elif cmd == "autogenerate":
        if len(sys.argv) < 3:
            print("✗ Укажите сообщение для миграции: python run_migrations.py autogenerate 'описание'")
            sys.exit(1)
        create_revision(sys.argv[2], autogenerate=True)
    else:
        print(f"✗ Неизвестная команда: {cmd}")
        print("Используйте: upgrade, downgrade, current, history, create, autogenerate")
        sys.exit(1)


if __name__ == "__main__":
    main()
