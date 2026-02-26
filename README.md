# arbitrage
Arbitrage platform

## Инфраструктура (dev)

Запуск PostgreSQL и Redis:

```bash
docker compose up -d
```

Убедитесь, что Docker запущен (Docker Desktop на WSL2 или `sudo systemctl start docker` в Linux).

Порты в `docker-compose.yml`: PostgreSQL **5433**, Redis **6378** (чтобы не конфликтовать с уже занятыми 5432/6379). В `.env` укажите:

- `DB_PORT=5433`
- `REDIS_PORT=6378`

Проверка контейнеров:

```bash
docker compose ps
docker compose logs -f
```
