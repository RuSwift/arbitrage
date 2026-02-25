import dataclasses
import datetime
import hashlib
import asyncio
import json
import logging
import random
from collections import defaultdict
from typing import List, Tuple, Optional, Dict, Type

import settings.base
from core.defs import CurrencyPair
from core.utils import utc_now_float, print_timeout
from core.p2p import P2PService, TradeSide, P2PAdvert, P2PAdvertInfo
from core.crud.intf.p2p import CRUDAdvertisers, CRUDAdverts, P2PAdvertsHeap
from core.crud.resources.p2p import (
    Advertiser as ResourceAdvertiser,
    AdvertInfo as ResourceAdvertInfo,
    AdvPosition as ResourceAdvPosition,
    CupStat as ResourceCupStat
)
from core.exceptions import BrowserOperationError
from core.connectors import BaseExchangeConnector, BookTicker, BookDepth
from core.repository import Repository
from connectors.defs import (
    build_tickers_key, build_book_ticker_key, build_book_depth_key
)
from core.lru import LRUCache
from foreground.p2p.stats import P2PLiquidityCalculator
from strategies.p2p.base import BaseStatsCollector
from strategies.exchanges.crypto import Crypto as CryptoRatios
from strategies.exchanges.forex import Forex as ForexRatios


TOKEN = str


class Publisher:
    """Публикация данных в Repository из коннектора биржи
    """

    MSG_QUEUE_JOB_SZ = 20

    class ConnectorCb(BaseExchangeConnector.Callback):

        def __init__(
                self, repo: Repository, exchange_code: str,
                loop: asyncio.AbstractEventLoop, queue: asyncio.Queue
        ):
            self.repo = repo
            self.exchange_code = exchange_code
            self.loop = loop
            self.queue = queue

        def handle(self, book: BookTicker = None, depth: BookDepth = None):
            if book:
                key = build_book_ticker_key(self.exchange_code, book.symbol)
                try:
                    self.queue.put_nowait([key, book.as_json()])
                except asyncio.QueueFull as e:
                    logging.warning(f'Full msg queue for {key}', e)

    def __init__(
        self, connector: BaseExchangeConnector,
        repo: Repository = None, symbols: List[str] = None, request_depth: bool = True
    ):
        self.__repo = repo or Repository(conn_pool=settings.base.REDIS_CONN_POOL)
        self.__connector = connector
        self.__symbols = symbols
        self.__msg_queue = asyncio.Queue(maxsize=8000)
        self.__request_depth = request_depth

    @property
    def symbols(self) -> List[str]:
        return self.__symbols

    @property
    def connector(self) -> BaseExchangeConnector:
        return self.__connector

    async def fire_tickers(self):
        tickers = self.__connector.get_all_tickers()
        await self.__repo.set(
            key=build_tickers_key(self.__connector.exchange_id()),
            value={
                'tickers': [t.as_json() for t in tickers]
            }
        )

    async def run(self):
        distributed_lock_key = settings.base.DistributedLockNamespaces.build_uniq_name(
            namespace=settings.base.DistributedLockNamespaces.TICKER_PUBLISHER,
            exchange_code=self.__connector.exchange_id()
        )
        if self.__symbols:
            distributed_lock_key_suffix = hashlib.md5(str(sorted(self.__symbols)).encode()).hexdigest()
            distributed_lock_key += '/' + distributed_lock_key_suffix
        lock = self.__repo.new_lock(distributed_lock_key)
        print('Barrier try')
        async with lock.barrier(infinite=True, ignore_exceptions=False, timeout=10) as lock_task:
            print('Barrier entered')
            cb = self.ConnectorCb(
                self.__repo, self.__connector.exchange_id(),
                loop=asyncio.get_event_loop(), queue=self.__msg_queue
            )
            await self.fire_tickers()
            self.__connector.start(cb, symbols=self.__symbols, depth=self.__request_depth)
            msg_q_task = asyncio.create_task(self.process_msg_queue())
            try:
                await asyncio.wait(
                    [lock_task, msg_q_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                msg_q_task.cancel()
                self.__connector.stop()

    def main(self):
        asyncio.get_event_loop().run_until_complete(self.run())

    async def process_msg_queue(self):

        async def _routine(q: asyncio.Queue, repo: Repository):
            while True:
                try:
                    key, value = await q.get()
                    await repo.publish(key, value)
                    logging.info(
                        msg=f'Connector event: publish to repo key: {key}',
                        extra=value
                    )
                except Exception as e:
                    logging.exception('REDIS PUB ERR: ', e)

        tasks = [
                    _routine(self.__msg_queue, self.__repo)
                ] * self.MSG_QUEUE_JOB_SZ

        await asyncio.gather(*tasks)


class P2PPublisher:
    """Публикация данных P2P биржи"""

    STEP_TIMEOUT_SEC = 60  # каждые 60 сек
    PAGES_DEPTH = 20
    TOR_LOCK_TIMEOUT_SEC = 3
    STORE_ADV_INFO_TO_DB = False
    USE_REVERSE_SERVICE_FACTOR = 3
    TOLERANCE_TO_PARALLEL_JOBS = False
    TOKENS_WEIGHTS = {'USDT': 50, 'BTC': 100, 'ETH': 500, 'USDC': 1000}

    @dataclasses.dataclass(frozen=True)
    class Job:
        epoch: float
        token: str
        sell: List[P2PAdvert]
        buy: List[P2PAdvert]
        page_no: int
        exc: Optional[Exception] = None

    def __init__(
        self, service_cls: Type[P2PService], fiat: str, exchange_code: str,
        repo: Repository = None, pages_depth: int = None,
        step_timeout_sec: int = None,
        reserved_service_list: List[Type[P2PService]] = None
    ):
        self.__repo = repo or Repository(conn_pool=settings.base.REDIS_CONN_POOL)
        self.__service_cls = service_cls
        self.__fiat = fiat
        self.__exchange_code = exchange_code
        self.__pages_depth = pages_depth or self.PAGES_DEPTH
        self.__step_timeout_sec = step_timeout_sec or self.STEP_TIMEOUT_SEC
        self.__crypto_ratios = CryptoRatios(repo=repo)
        self.__forex_ratios = ForexRatios(repo=repo)
        self.__reserved_service_list = reserved_service_list or []

    @property
    def fiat(self) -> str:
        return self.__fiat

    async def run(
        self,
        crud_advertiser: CRUDAdvertisers = None,
        crud_adverts: CRUDAdverts = None
    ):
        crud_advertiser = crud_advertiser or self.__repo
        crud_adverts = crud_adverts or self.__repo

        distributed_lock_key = settings.base.DistributedLockNamespaces.build_uniq_name(
            namespace=settings.base.DistributedLockNamespaces.P2P_PUBLISHER,
            exchange_code=self.__exchange_code,
            fiat=self.__fiat
        )
        lock = self.__repo.new_lock(distributed_lock_key)
        print('Barrier try')
        async with lock.barrier(infinite=True, ignore_exceptions=False, timeout=10) as lock_task:
            print('Barrier entered ok...')
            processor_task = asyncio.create_task(
                self.process(
                    fiat=self.__fiat, service_cls=self.__service_cls,
                    crud_advertiser=crud_advertiser,
                    crud_adverts=crud_adverts,
                    reserved_service_list=self.__reserved_service_list
                )
            )
            try:
                try:
                    await asyncio.wait(
                        [lock_task, processor_task],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                except Exception as e:
                    logging.exception('EXC: ' + str(e))
                    raise
            finally:
                processor_task.cancel()

    async def process(
        self, fiat: str, service_cls: Type[P2PService],
        crud_advertiser: CRUDAdvertisers, crud_adverts: CRUDAdverts,
        reserved_service_list: List[Type[P2PService]]
    ):
        async def _load_pages(
            token_: str, job_queue_: list, cur_epoch_: float, lock_key: str = None
        ):
            sell_pages_done = False
            buy_pages_done = False
            service_ = service_cls()
            try:
                for pg in range(1, self.__pages_depth):
                    logging.info(f'> Page {pg}/{self.__pages_depth} for token {token_} fiat: {fiat}')
                    try:
                        if lock_key:
                            await self.__repo.set(
                                lock_key, {'on': True},
                                ttl=self.TOR_LOCK_TIMEOUT_SEC
                            )
                        try:
                            sell_page = []
                            buy_page = []
                            if sell_pages_done and buy_pages_done:
                                logging.info(f'\tFinish all pages for token: {token_} fiat: {fiat}')
                                break
                            if not sell_pages_done:
                                sell_page = await service_.load_adv_page(
                                    side=TradeSide.SELL,
                                    fiat=self.fiat, token=token_, page=pg
                                )
                                sell_pages_done = len(sell_page) == 0
                                logging.info(f'\tsell page len for token: {token_} is {len(sell_page)}  fiat: {fiat}')
                            if not buy_pages_done:
                                buy_page = await service_.load_adv_page(
                                    side=TradeSide.BUY,
                                    fiat=self.fiat, token=token_, page=pg
                                )
                                buy_pages_done = len(buy_page) == 0
                                logging.info(
                                    f'\tbuy page len for token: {token_} is {len(buy_page)} fiat: {fiat}'
                                )
                            if sell_page or buy_page:
                                logging.info(f'\tadd pages to job for token {token_} fiat: {fiat}')
                                job_queue_.append(
                                    self.Job(
                                        epoch=cur_epoch_, token=token_,
                                        sell=sell_page, buy=buy_page,
                                        page_no=pg
                                    )
                                )
                        finally:
                            if lock_key:
                                pass
                    except Exception as e:
                        job_queue.append(
                            self.Job(
                                epoch=cur_epoch_, token=token_,
                                sell=[], buy=[],
                                exc=e, page_no=pg
                            )
                        )
                        if isinstance(e, BrowserOperationError):
                            if e.error_code in [429, 504]:
                                logging.warning(f'\tHTTP Error {e.error_code} Token: [{token_}] fiat: {fiat}')
                                break
                        logging.exception(f'\tP2P Exc Token: [{token_}] fiat: {fiat} page: {pg}')
                        break
            finally:
                await service_.terminate()

        async def _load_pages_in_sequence(
            tokens_: List[str], job_queue_: list,
            cur_epoch_: float, lock_key_: str = None
        ):
            for token_ in tokens_:
                await _load_pages(token_, job_queue_, cur_epoch_, lock_key_)

        stamp_begin = None
        rebuild_tor_circuit = True
        avail_pay_methods = set()
        tor_lock_key = None
        liquidity = P2PLiquidityCalculator(
            self.__exchange_code, crud=crud_adverts, lru_size=3000
        )
        forex_ratio: Optional[CurrencyPair] = None
        crypto_ratios: Dict[TOKEN, CurrencyPair] = {}
        advertisers_lru = LRUCache(capacity=1000)
        error_429_counter = 0
        while True:
            cup_has_errors = False
            if stamp_begin:
                now = datetime.datetime.now()
                delta = now - stamp_begin
                if delta.total_seconds() < self.__step_timeout_sec:
                    sleep_timeout = self.__step_timeout_sec - delta.total_seconds()
                    logging.info(f'delay for {sleep_timeout} seconds...')
                    await asyncio.sleep(sleep_timeout)
            try:
                service = None
                if error_429_counter > self.USE_REVERSE_SERVICE_FACTOR:
                    error_429_counter = 0
                    if reserved_service_list:
                        logging.info('Use reserved service')
                        service = random.choice(reserved_service_list)()
                if service is None:
                    service = service_cls()
                if service.cmd:
                    logging.info('P2P service has Tor CMD interface')
                else:
                    logging.info('P2P service Dont has Tor CMD interface')
                if service.cmd and rebuild_tor_circuit:
                    tor_lock_key = settings.base.DistributedLockNamespaces.build_uniq_name(
                        namespace=settings.base.DistributedLockNamespaces.P2P_PUBLISHER,
                        exchange_code=self.__exchange_code,
                        tor_host=service.cmd.host
                    )
                    logging.info('Rebuild Tor circuit')
                    locked = await self.__repo.get(tor_lock_key)
                    if not locked:
                        service.cmd.rebuild_nym()
                        rebuild_tor_circuit = False
                        await self.__repo.set(
                            tor_lock_key, {'on': True},
                            ttl=self.TOR_LOCK_TIMEOUT_SEC
                        )
                    else:
                        logging.info('tor locked by timeout')
                else:
                    logging.info('Skip rebuild Tor circuit')
                tokens = await service.load_tokens(fiat)
                tokens = sorted(
                    tokens, key=lambda t: self.TOKENS_WEIGHTS.get(
                        t.code, 10000000
                    )
                )
                await BaseStatsCollector.update_metadata(
                    exchange_code=self.__exchange_code,
                    metadata={
                        fiat: [t.code for t in tokens],
                        f'{fiat}__pay_methods': list(avail_pay_methods)
                    }
                )
                # fill forex & crypto ration
                print('')
                forex_ratio = await self.__forex_ratios.ratio(
                    base='USD', quote=fiat
                )
                crypto_ratios.clear()
                for token in tokens:
                    crypto_ratios[token.code] = await self.__crypto_ratios.ratio(
                        base='USDT', quote=token.code
                    )
            except Exception as e:
                logging.exception('EXC: loading tokens')
                raise e
            logging.info(f'******* FIAT: {fiat} **********')
            logging.info(
                'P2P tokens: %s' % ','.join([t.code for t in tokens])
            )
            job_queue: List['P2PPublisher.Job'] = list()
            epoch = utc_now_float()
            if self.TOLERANCE_TO_PARALLEL_JOBS:
                tasks = [
                    asyncio.create_task(
                        _load_pages(
                            token_=t.code, job_queue_=job_queue,
                            cur_epoch_=epoch, lock_key=tor_lock_key
                        )
                    )
                    for t in set(tokens)
                ]
            else:
                tasks = [
                    asyncio.create_task(
                        _load_pages_in_sequence(
                            tokens_=[t.code for t in tokens],
                            job_queue_=job_queue,
                            cur_epoch_=epoch,
                            lock_key_=tor_lock_key
                        )
                    )
                ]
            stamp_begin = datetime.datetime.now()
            try:
                try:
                    await asyncio.wait(
                        tasks, timeout=self.__step_timeout_sec
                    )
                finally:
                    for tsk in tasks:
                        tsk.cancel()
            except Exception as e:
                logging.exception('Exc Tasks')
                raise e
            stamp_end = datetime.datetime.now()
            try:
                logging.info(f'> Build database transactions to commit fiat: {fiat}')
                advertisers: Dict[str, ResourceAdvertiser] = {}
                for job in job_queue:
                    logging.warning(
                        f'JOB token = {job.token} epoch = {job.epoch}'
                        f' sells: {len(job.sell)} buys: {len(job.buy)}'
                        f' exc: {job.exc}'
                    )
                    if job.exc:
                        rebuild_tor_circuit = True
                        if '429' in str(job.exc):
                            logging.info(f'Register 429 error')
                            error_429_counter += 1
                        cup_has_errors = True
                        logging.warning('Register Cup Error')
                    for adv in job.buy + job.sell:
                        if adv.advertiser.uid not in advertisers.keys():
                            a = ResourceAdvertiser(
                                exchange_code=self.__exchange_code,
                                uid=adv.advertiser.uid,
                                nick=adv.advertiser.nick,
                                rating=adv.advertiser.rating,
                                finish_rate=adv.advertiser.finish_rate,
                                trades_month=adv.advertiser.trades_month,
                                trades_total=adv.advertiser.trades_total,
                                extra=adv.advertiser.extra,
                                verified_merchant=adv.advertiser.verified_merchant,
                                recommended=adv.advertiser.recommended
                            )
                            advertisers[adv.advertiser.uid] = a
                logging.info('\trefresh advertisers info')
                with print_timeout('\tadvertisers db ops estimated: '):
                    # сохраняем только измененные записи
                    advertisers_to_update: List[ResourceAdvertiser] = []
                    print(f'\t advertisers been collected: {len(advertisers.values())} count')
                    for a in advertisers.values():
                        rec = advertisers_lru.get(a.data_hash())
                        if rec is None:
                            advertisers_to_update.append(a)
                    print(f'\t advertisers to store: {len(advertisers_to_update)} count')
                    await crud_advertiser.advertiser_store(
                        a=advertisers_to_update
                    )
                    for a in advertisers_to_update:
                        advertisers_lru.put(a.data_hash(), a)
                with print_timeout(f'\tadverts db ops estimated: '):
                    txns: List[crud_adverts.AdvTransaction] = []
                    cur_sell_pos: Dict[str, int] = defaultdict(int)
                    cur_buy_pos: Dict[str, int] = defaultdict(int)
                    for job in job_queue:
                        for sell in job.sell:
                            pos = cur_sell_pos[job.token]
                            pos += 1
                            cur_sell_pos[job.token] = pos
                            advert = self._build_adv_resource(
                                info=sell.info,
                                advertiser=advertisers.get(sell.advertiser.uid),
                                side=TradeSide.SELL,
                                epoch=epoch,
                                fiat=fiat,
                                token=job.token
                            )
                            avail_pay_methods.update(
                                set(sell.info.pay_methods)
                            )
                            txns.append(
                                crud_adverts.AdvTransaction(
                                    position=pos,
                                    advertiser_uid=sell.advertiser.uid,
                                    epoch=epoch,
                                    adv=advert
                                )
                            )
                        for buy in job.buy:
                            pos = cur_buy_pos[job.token]
                            pos += 1
                            cur_buy_pos[job.token] = pos
                            advert = self._build_adv_resource(
                                info=buy.info,
                                advertiser=advertisers.get(buy.advertiser.uid),
                                side=TradeSide.BUY,
                                epoch=epoch,
                                fiat=fiat,
                                token=job.token
                            )
                            avail_pay_methods.update(
                                set(buy.info.pay_methods)
                            )
                            txns.append(
                                crud_adverts.AdvTransaction(
                                    position=pos,
                                    advertiser_uid=buy.advertiser.uid,
                                    epoch=epoch,
                                    adv=advert
                                )
                            )
                    adv_positions: List[ResourceAdvPosition]
                    if self.STORE_ADV_INFO_TO_DB:
                        adv_positions = await crud_adverts.adv_append_multiple(
                            txns=txns, compress=True
                        )
                    else:
                        adv_positions = await crud_adverts.adv_to_position_multiple(
                            txns=txns
                        )
                with print_timeout(f'\tadverts stat ops estimated: '):
                    stats: Dict[str, ResourceCupStat] = {
                        token.code: ResourceCupStat(
                            fiat=fiat,
                            epoch=epoch,
                            token=token.code,
                            utc=utc_now_float()
                        )
                        for token in tokens
                    }
                    for pos in adv_positions:
                        stat = stats.get(pos.adv.token)
                        stat.append(pos)

                    stats_as_json = {
                        token: stat.to_dict() for token, stat in stats.items()
                    }
                    if not cup_has_errors:
                        await self.__repo.set(
                            settings.base.DistributedLockNamespaces.p2p_cup_key(
                                exchange_code=self.__exchange_code,
                                fiat=self.fiat
                            ),
                            value=stats_as_json
                        )
                    else:
                        logging.warning('Ignore save Cup Info cause of registered cup error')
                with print_timeout(f'\tcalc liquidity: '):
                    events = await liquidity.extract_events(adv_positions)
                    print('============== TRADE EVENTS ==================')
                    notify_channel = settings.base.DistributedLockNamespaces.p2p_trade_events_channel()
                    for e in events:
                        stat = stats.get(e.token)
                        if stat:
                            flt = stat.filter(e.pay_methods, e.advertiser)
                            me, _ = flt.find(e.adv_uid)
                            if me:
                                e.counterparty_position = me.index
                        crypto_ratio = crypto_ratios.get(e.token)
                        if forex_ratio:
                            e.forex_ratio = forex_ratio.ratio
                            if crypto_ratio:
                                e.usdt_price = e.price / crypto_ratio.ratio
                    # publish to redis
                    if events:
                        data = {
                            'exchange': self.__exchange_code,
                            'events': [e.model_dump(mode='json') for e in events]
                        }
                        await self.__repo.notify(key=notify_channel, value=data)
                    print(f'\tProcessed {len(events)} trade events')
                    if events:
                        await crud_adverts.event_append_multiple(
                            exchange_code=self.__exchange_code,
                            events=events
                        )
                    print('\tTRade events saved to db')
                    print('==============================================')

            except Exception as e:
                logging.exception('Exc processing job')
                raise e

    @classmethod
    def _build_adv_resource(
        cls, info: P2PAdvertInfo, advertiser: ResourceAdvertiser,
        side: TradeSide, epoch: float, fiat: str, token: str
    ) -> ResourceAdvertInfo:
        return ResourceAdvertInfo(
            uid=info.uid,
            advertiser=advertiser,
            price=info.price,
            quantity=info.quantity,
            quantity_fiat=info.quantity * info.price,
            min_qty=info.min_max_qty[0],
            max_qty=info.min_max_qty[1],
            pay_methods=set(info.pay_methods),
            epoch=epoch,
            extra=info.extra,
            fiat=fiat,
            side=side,
            token=token
        )
