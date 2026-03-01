"""Сервис обхода perpetual (фьючерсных) котировок по CEX."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session as SyncDBSession

from app.cex.base import BaseCEXPerpetualConnector
from app.cex.orcestrator import PerpetualOrchestratorImpl

if TYPE_CHECKING:
    from redis import Redis

from app.cex.dto import CurrencyPair
from app.db.models import CrawlerIteration, CrawlerJob, Token
from app.services.base import BaseService
from app.services.unit_of_work import UnitOfWork
from app.settings import ServiceConfigRegistry


class CEXPerpetualCrawler(BaseService):
    """Обход perpetual-рынка биржи: пары, стакан, свечи, funding rate."""

    class Config(BaseModel, extra="ignore"):
        """Конфиг краулера. Задаётся при создании или из БД (service_config)."""

        align_to_minutes: int = 1  # выравнивание timestamp до N минут
        cache_timeout: float = 15.0  # TTL кеша в Redis для оркестратора
        funding_rate_window_min: int = 15  # окно для расчёта funding rate
        funding_history_window_min: int = 60 * 3  # окно для расчёта истории funding rate
        funding_min_abs_rate_percents: float = 0.3  # минимальный абсолютный funding rate в %
        liquidity_book_window_min: int = 30  # окно для расчёта ликвидности в стакане
        liquidity_book_depth_factor: int = 5  # лимит depth для стакана при рассчете допустимой ликвидности
        liquidity_book_amount_factor: int = 1000  # лимит ликвидности в стакане в USD
        

    ConfigModel: type[BaseModel] = Config
    kind = "perpetual"

    def __init__(
        self,
        uow: UnitOfWork,
        exchange_id: str,
        config: Config | None = None,
    ) -> None:
        super().__init__(uow)
        self._exchange_id = exchange_id
        self._config = config
        self._connector: BaseCEXPerpetualConnector | None = None

    async def _resolve_config(self) -> Config:
        """Конфиг: переданный в __init__, из БД (CEXPerpetualCrawler) или дефолтный."""
        if self._config is not None:
            return self._config
        loaded = await ServiceConfigRegistry.aget(
            self.db, "CEXPerpetualCrawler", self.__class__.Config
        )
        return loaded or self.__class__.Config()

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
    
    async def prepare_job(self) -> CrawlerJob:
        """Upsert CrawlerJob по (exchange, kind): найти по exchange_id и kind=perpetual, иначе создать."""
        result = await self.db.execute(
            select(CrawlerJob).where(
                CrawlerJob.exchange == self._exchange_id,
                CrawlerJob.kind == self.kind,
            )
        )
        job = result.scalar_one_or_none()
        now = datetime.now(timezone.utc)
        if job is not None:
            job.start = now
            job.stop = None
            job.error = None
            await self.db.flush()
            await self.db.refresh(job)
            return job
        job = CrawlerJob(
            exchange=self._exchange_id,
            connector=self.kind,
            kind=self.kind,
            start=now,
            stop=None,
            error=None,
        )
        self.db.add(job)
        await self.db.flush()
        await self.db.refresh(job)
        return job
    
    def prepare_job_iterations(
        self,
        job_id: int,
        db: SyncDBSession,
        redis: "Redis",
        config: Config,
    ) -> list[CrawlerIteration]:
        """Синхронно: коннектор + загрузка токенов и upsert итераций через переданную SyncDBSession, Sync Redis и конфиг. job_id — id CrawlerJob (в поток не передавать ORM-объекты от async-сессии)."""
        result = db.execute(select(Token).order_by(Token.id))
        tokens = list(result.scalars().all())
        symbols_ordered = list(dict.fromkeys(t.symbol for t in tokens))

        connector = self._get_connector()
        all_perpetuals = connector.get_all_perpetuals()
        tokens_set = set(symbols_ordered)
        tickers_in_scope = [t for t in all_perpetuals if t.base in tokens_set]
        symbols_for_get_pairs = [t.exchange_symbol for t in tickers_in_scope]
        pairs = connector.get_pairs(symbols=symbols_for_get_pairs)
        pair_by_base: dict[str, CurrencyPair] = {p.base: p for p in (pairs or [])}
        bases_on_exchange = {t.base for t in all_perpetuals}

        now = datetime.now(timezone.utc)
        iterations: list[CrawlerIteration] = []

        for symbol in symbols_ordered:
            r = db.execute(
                select(CrawlerIteration).where(
                    CrawlerIteration.crawler_job_id == job_id,
                    CrawlerIteration.token == symbol,
                )
            )
            it = r.scalar_one_or_none()
            if it is None:
                it = CrawlerIteration(
                    crawler_job_id=job_id,
                    token=symbol,
                    start=now,
                    stop=None,
                    done=False,
                    status="init",
                    last_update=now,
                )
                db.add(it)
                db.flush()

            it.last_update = now
            if symbol in pair_by_base:
                p = pair_by_base[symbol]
                it.symbol = p.code
                it.currency_pair = p.as_dict()
                if it.inactive_till_timestamp is None or it.inactive_till_timestamp <= now:
                    if it.status != "success":
                        it.status = "pending"
                    it.start = now
                    it.stop = None
                    it.comment = None
                publisher = PerpetualOrchestratorImpl(
                    db_session=db,
                    redis=redis,
                    exchange_id=self._exchange_id,
                    kind=self.kind,
                    symbol=p.code,
                    cache_timeout=config.cache_timeout,
                    align_to_minutes=config.align_to_minutes,
                )
                publisher.publish_price(p)
            else:
                it.status = "ignore"
                it.comment = "missing in ex platform" if symbol not in bases_on_exchange else "missing in tokens list"
            iterations.append(it)

        db.flush()
        for it in iterations:
            db.refresh(it)
        return [it for it in iterations if it.status == "pending"]

    def _redis_window_key(self, window_type: str, symbol: str) -> str:
        """Ключ Redis для окна (значение с TTL, по истечении — повторный запрос разрешён)."""
        safe_sym = (symbol or "").replace("/", "_")
        return f"arbitrage:crawler:{self.kind}:window:{window_type}:{self._exchange_id}:{safe_sym}"

    _WINDOW_KEY_MAGIC = "1"  # значение ключа новой схемы (с TTL); иное — старая схема (timestamp)

    def _may_fetch_by_window(self, redis: "Redis", key: str, window_min: int) -> bool:
        """True если ключ отсутствует (или истёк по TTL); при True выставляет ключ с TTL = window_min минут.
        Ключи с другим значением (старая схема с timestamp) или без TTL считаются устаревшими — удаляются и запрос разрешается."""
        raw = redis.get(key)
        if raw is not None:
            if raw == self._WINDOW_KEY_MAGIC and redis.ttl(key) != -1:
                return False
            redis.delete(key)
        redis.setex(key, window_min * 60, self._WINDOW_KEY_MAGIC)
        return True

    def _liquidity_usd_top_n(
        self,
        bids: list,
        asks: list,
        n: int,
        price_ratio: float,
    ) -> tuple[float, float]:
        """Сумма в USD по топ n уровней с каждой стороны. quantity в токенах (base), пересчёт в USD: price_ratio * quantity. price_ratio — курс base в quote (USD)."""
        if price_ratio <= 0:
            return 0.0, 0.0

        def side_usd(rows: list, desc: bool) -> float:
            sorted_rows = sorted(rows, key=lambda x: float(x.get("price", 0) or 0), reverse=desc)[:n]
            return sum(
                price_ratio * float(r.get("quantity", 0) or 0)
                for r in sorted_rows
            )
        bid_usd = side_usd(bids or [], True)
        ask_usd = side_usd(asks or [], False)
        return bid_usd, ask_usd

    def run_once(
        self,
        iter_id: int,
        db: SyncDBSession,
        redis: "Redis",
        config: Config,
    ) -> None:
        """Один проход по итерации: загрузка из БД, funding/depth с учётом окон в Redis, проверка ликвидности и funding, при необходимости — inactive."""
        now_utc = datetime.now(timezone.utc)
        result = db.execute(select(CrawlerIteration).where(CrawlerIteration.id == iter_id))
        it = result.scalar_one_or_none()
        if it is None:
            self.log.warning("run_once: CrawlerIteration id=%s not found", iter_id)
            return
        try:
            self._run_once_impl(it, now_utc, db, redis, config)
            if it.status == "pending":
                it.status = "success"
            it.stop = now_utc
            db.flush()
        except Exception as e:
            it.error = str(e)
            it.status = "error"
            it.stop = now_utc
            it.last_update = now_utc
            db.flush()

    def _run_once_impl(
        self,
        it: CrawlerIteration,
        now_utc: datetime,
        db: SyncDBSession,
        redis: "Redis",
        config: Config,
    ) -> None:
        """Внутренняя реализация run_once без обработки исключений."""
        symbol = it.symbol or CurrencyPair.build_code(it.token, "USDT")
        connector = self._get_connector()
        inactive_until_book = now_utc + timedelta(minutes=config.liquidity_book_window_min)
        inactive_until_funding = now_utc + timedelta(minutes=config.funding_rate_window_min)
        price_ratio = 0.0
        if it.currency_pair and isinstance(it.currency_pair, dict):
            price_ratio = float(it.currency_pair.get("ratio") or 0)
        if price_ratio <= 0.0:
            raise ValueError(
                f"CrawlerIteration id={it.id} symbol={symbol}: currency_pair.ratio missing or zero, cannot compute liquidity in USD"
            )

        # Funding rate — не чаще funding_rate_window_min
        key_fr = self._redis_window_key("funding_rate", symbol)
        if self._may_fetch_by_window(redis, key_fr, config.funding_rate_window_min):
            fr = connector.get_funding_rate(symbol)
            if fr is not None:
                it.funding_rate = fr.as_dict()
                if getattr(fr, "next_rate", None) is not None:
                    it.next_funding_rate = {"next_funding_utc": fr.next_funding_utc, "next_rate": fr.next_rate}
        # История фандинга — не чаще funding_history_window_min
        key_hist = self._redis_window_key("funding_history", symbol)
        if self._may_fetch_by_window(redis, key_hist, config.funding_history_window_min):
            hist = connector.get_funding_rate_history(symbol, limit=50)
            if hist is not None:
                it.funding_rate_history = [p.as_dict() for p in hist]

        # BookDepth — не чаще liquidity_book_window_min
        key_book = self._redis_window_key("book_depth", symbol)
        book_fetched = self._may_fetch_by_window(redis, key_book, config.liquidity_book_window_min)
        if book_fetched:
            depth = connector.get_depth(symbol, limit=max(config.liquidity_book_depth_factor * 2, 20))
            if depth is not None:
                it.book_depth = depth.as_dict()
                bids = getattr(depth, "bids", []) or []
                asks = getattr(depth, "asks", []) or []
                bid_list = [{"price": b.price, "quantity": b.quantity} for b in bids]
                ask_list = [{"price": a.price, "quantity": a.quantity} for a in asks]
                bid_usd, ask_usd = self._liquidity_usd_top_n(
                    bid_list, ask_list, config.liquidity_book_depth_factor, price_ratio
                )
                min_liquidity = min(bid_usd, ask_usd)
                if min_liquidity < config.liquidity_book_amount_factor:
                    it.inactive_till_timestamp = inactive_until_book
                    it.status = "inactive"
                    it.comment = "insufficient liquidity in order book"
                    it.last_update = now_utc
                    db.flush()
                    return

        # Проверка ликвидности по уже сохранённому стакану (если не запрашивали только что)
        if not book_fetched and it.book_depth and isinstance(it.book_depth, dict):
            raw_bids = (it.book_depth.get("bids") or [])
            raw_asks = (it.book_depth.get("asks") or [])
            bid_usd, ask_usd = self._liquidity_usd_top_n(
                raw_bids, raw_asks, config.liquidity_book_depth_factor, price_ratio
            )
            if min(bid_usd, ask_usd) < config.liquidity_book_amount_factor:
                it.inactive_till_timestamp = inactive_until_book
                it.status = "inactive"
                it.comment = "insufficient liquidity in order book"
                it.last_update = now_utc
                db.flush()
                return

        # Проверка минимального |funding| в %
        fr_data = it.funding_rate
        if fr_data is None or not isinstance(fr_data, dict):
            it.inactive_till_timestamp = inactive_until_funding
            it.status = "inactive"
            it.comment = "no funding rate data"
            it.last_update = now_utc
            db.flush()
            return
        rate = float(fr_data.get("rate", 0) or 0)
        rate_pct = abs(rate) * 100
        if rate_pct < config.funding_min_abs_rate_percents:
            it.inactive_till_timestamp = inactive_until_funding
            it.status = "inactive"
            it.comment = f"funding rate {rate_pct:.2f}% below minimum absolute threshold"
            it.last_update = now_utc
            db.flush()
            return

        it.last_update = now_utc
        db.flush()
