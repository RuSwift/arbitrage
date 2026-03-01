# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.8.3 \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false

ENV PATH="${POETRY_HOME}/bin:${PATH}"

# Install system deps: poetry + supervisord
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    supervisor \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root --only main

# Application code
COPY app ./app
COPY scripts ./scripts
COPY alembic ./alembic
COPY alembic.ini run_migrations.py ./

# Supervisord config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8000

# 1 GB virtual memory limit for supervisord and all child processes (web + crawler2)
ENV SUPERVISORD_MEMORY_LIMIT_KB=1048576
CMD ["/bin/bash", "-c", "ulimit -v ${SUPERVISORD_MEMORY_LIMIT_KB} && exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf"]
