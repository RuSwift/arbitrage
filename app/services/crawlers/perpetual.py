"""Сервис обхода perpetual (фьючерсных) котировок по CEX."""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.cex.base import BaseCEXPerpetualConnector
from app.services.base import BaseService
from app.services.unit_of_work import UnitOfWork

if TYPE_CHECKING:
    from app.db.models import CrawlerJob


class CEXPerpetualCrawler(BaseService):
    """Обход perpetual-рынка биржи: пары, стакан, свечи, funding rate."""

    def __init__(self, uow: UnitOfWork, exchange_id: str) -> None:
        super().__init__(uow)
        self._exchange_id = exchange_id
        self._connector: BaseCEXPerpetualConnector | None = None

    def _get_connector(self) -> BaseCEXPerpetualConnector:
        """Ленивое создание коннектора по exchange_id через BaseCEXPerpetualConnector.Registry."""
        if self._connector is None:
            connector_cls = BaseCEXPerpetualConnector.Registry.get(self._exchange_id)
            if connector_cls is None:
                raise ValueError(
                    f"Unknown exchange_id={self._exchange_id!r}. "
                    f"Available: {list(BaseCEXPerpetualConnector.Registry.keys())}"
                )
            self._connector = connector_cls(log=self.log)
        return self._connector

    async def run(self, job: CrawlerJob | None = None) -> None:
        """Запуск обхода. job — существующий CrawlerJob или None (создать новый)."""
        raise NotImplementedError("Implement run(): load tokens, create iterations, fetch pairs/depth/klines/funding")
