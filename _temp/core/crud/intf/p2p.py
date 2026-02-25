import hashlib
import dataclasses
import json
from functools import lru_cache
from typing import Optional, List, Set, Literal, Union, Dict, Tuple

from channels.db import database_sync_to_async as sync_to_async
from django.db import transaction
from django.db.models import Q

from core.models import (
    P2PAdvertiser as DBP2PAdvertiser,
    P2PAdvertInfo as DBP2PAdvertInfo,
    P2PAdvertPosition as DBP2PAdvertPosition,
    P2PEpoch as DBP2PEpoch,
    P2PTradeEvent as DBP2PTradeEvent
)
from core.lru import LRUCache
from core.p2p import TradeSide
from core.crud.base import CRUD
from core.crud.resources.p2p import (
    Advertiser, AdvertInfo, AdvPosition, AdvEpochInfo, TradeEvent
)


class CRUDAdvertisers(CRUD):

    async def advertiser_store(self, a: Union[Advertiser, List[Advertiser]]):
        await sync_to_async(self.__store)(a)
        await self._on_modified(target=a)

    async def advertiser_exists(self, uid: str) -> bool:
        return await sync_to_async(self.__is_exists)(uid)

    async def advertiser_get(self, uid: str) -> Advertiser:
        return await sync_to_async(self.__get)(uid)

    @staticmethod
    def __is_exists(uid: str) -> bool:
        return DBP2PAdvertiser.objects.filter(uid=uid).exists()

    @staticmethod
    def __get(uid: str) -> Advertiser:
        rec = DBP2PAdvertiser.objects.get(uid=uid)
        return Advertiser(
            exchange_code=rec.exchange_code,
            uid=rec.uid,
            nick=rec.nick,
            rating=rec.rating,
            finish_rate=rec.finish_rate,
            trades_month=rec.trades_month,
            trades_total=rec.trades_total,
            extra=rec.extra,
            verified_merchant=rec.verified_merchant,
            recommended=rec.recommended
        )

    @staticmethod
    def __store(aa: Union[Advertiser, List[Advertiser]]):
        if isinstance(aa, Advertiser):
            values = [aa]
        else:
            values = aa
        for val in values:
            defaults = dict(
                exchange_code=val.exchange_code,
                nick=val.nick,
                rating=val.rating,
                finish_rate=val.finish_rate,
                trades_month=val.trades_month,
                trades_total=val.trades_total,
                hash=val.data_hash(),
                extra=val.extra or {},
                verified_merchant=val.verified_merchant,
                recommended=val.recommended
            )
            DBP2PAdvertiser.objects.update_or_create(
                defaults=defaults, uid=val.uid
            )


class CRUDAdverts(CRUD):

    @dataclasses.dataclass
    class AdvFilters:
        uid: Union[str, List[str]] = None
        advertiser_uid: str = None
        advertiser_rating: float = None
        advertiser_finish_rate: float = None
        advertiser_total_trades: int = None
        advertiser_trades: int = None
        position: int = None
        epoch_eq: float = None
        epoch_gt: float = None
        epoch_lt: float = None
        epoch_from: float = None
        quantity_gt: float = None
        quantity_lt: float = None
        exchange_code: str = None
        fiat: str = None
        pay_methods: Set[str] = None
        pay_methods_eq: Set[str] = None
        side: Literal[TradeSide.BUY, TradeSide.SELL] = None
        token: str = None
        last_only: bool = False  # только последние записи для каждого Adv

    @dataclasses.dataclass
    class AdvTransaction:
        position: int
        advertiser_uid: str
        epoch: float
        adv: AdvertInfo

    @dataclasses.dataclass
    class TradeEventFilters:
        pk_offset: int = None
        adv_uid: Union[str, List[str]] = None
        advertiser_uid: str = None
        epoch_from: Optional[float] = None
        exchange_code: str = None
        fiat: str = None
        token: str = None

    async def adv_append(
        self, advertiser_uid: str, position: int,
        epoch: float, adv: AdvertInfo, compress: bool = False
    ) -> AdvPosition:
        return await sync_to_async(
            self.__adv_append
        )(
            advertiser_uid=advertiser_uid,
            position=position,
            epoch=epoch,
            adv=adv,
            compress=compress
        )

    async def adv_append_multiple(
        self, txns: List[AdvTransaction], compress: bool = False
    ) -> List[AdvPosition]:
        return await sync_to_async(
            self.__adv_append_multiple
        )(
            txns=txns,
            compress=compress
        )

    @classmethod
    async def adv_to_position_multiple(
        cls, txns: List[AdvTransaction]
    ) -> List[AdvPosition]:
        poses: List[AdvPosition] = []
        for txn in txns:
            pos = AdvPosition(
                adv=txn.adv,
                position=txn.position,
                epoch=txn.epoch
            )
            poses.append(pos)
        return poses

    async def event_append_multiple(
        self, exchange_code: str, events: List[TradeEvent]
    ):
        if not events:
            return
        await sync_to_async(self.__trade_event_append_multiple)(
            exchange_code=exchange_code,
            events=events
        )

    async def adv_fetch_one(self, filters: AdvFilters, last: bool=True) -> Optional[AdvertInfo]:
        ret = await sync_to_async(self.__adv_fetch)(
            filters, reversed_=last is True, limit=1
        )
        return ret[0] if ret else None

    async def adv_fetch(self, filters: AdvFilters, reversed_: bool = False, limit: int = None) -> List[AdvertInfo]:
        return await sync_to_async(self.__adv_fetch)(
            filters, reversed_, limit
        )

    async def epoch_fetch(
        self, exchange_code: str, side: str, token: str, fiat: str,
        offset: int = None, size: int = None, reverse: bool = False
    ) -> List[AdvEpochInfo]:
        epochs = await sync_to_async(self.__epoch_fetch)(
            exchange_code=exchange_code, side=side, token=token, fiat=fiat,
            offset=offset, size=size, reverse=reverse
        )
        return epochs

    async def events_fetch(
        self, filters: TradeEventFilters, reverse: bool = False,
        limit: int = None
    ) -> Tuple[List[TradeEvent], Optional[int]]:
        fetched, offset = await sync_to_async(self.__events_fetch)(
            filters, reverse, limit
        )
        return fetched, offset

    async def ensure_epoch_metadata_stored(
        self, epoch: float, exchange_code: str, fiat: str,
        side: str, token: str, errors: Dict = None
    ) -> AdvEpochInfo:
        rec: DBP2PEpoch = await sync_to_async(self.__ensure_epoch_metadata_stored)(
            epoch=epoch, exchange_code=exchange_code, fiat=fiat,
            side=side, token=token, errors=errors
        )
        info = AdvEpochInfo(
            epoch=rec.epoch,
            fiat=rec.fiat,
            token=rec.token,
            errors=rec.errors or {}
        )
        return info

    async def pos_fetch(
        self, epoch: float, exchange_code: str,
        side: str, token: str, fiat: str
    ) -> List[AdvPosition]:
        result = await sync_to_async(self.__pos_fetch)(
            epoch=epoch, exchange_code=exchange_code,
            side=side, token=token, fiat=fiat
        )
        return result

    @staticmethod
    def __adv_get_advertiser(rec: Optional[DBP2PAdvertiser]) -> Optional[Advertiser]:
        if rec is None:
            return None
        else:
            return Advertiser(
                exchange_code=rec.exchange_code,
                uid=rec.uid,
                nick=rec.nick,
                rating=rec.rating,
                finish_rate=rec.finish_rate,
                trades_month=rec.trades_month,
                trades_total=rec.trades_total,
                extra=rec.extra,
                verified_merchant=rec.verified_merchant
            )

    @classmethod
    def __adv_get_info(cls, rec: Optional[DBP2PAdvertInfo]) -> Optional[AdvertInfo]:
        if rec is None:
            return None
        else:
            return AdvertInfo(
                uid=rec.uid,
                advertiser=cls.__adv_get_advertiser(rec.advertiser),
                price=rec.price,
                quantity=rec.quantity,
                quantity_fiat=rec.quantity_fiat,
                min_qty=rec.min_qty,
                max_qty=rec.max_qty,
                pay_methods=set(rec.pay_methods),
                epoch=rec.epoch,
                duplicate_epochs=list(rec.duplicate_epochs),
                extra=rec.extra,
                fiat=rec.fiat,
                side=rec.side,
                token=rec.token
            )

    @classmethod
    def __event_extract(cls, rec: Optional[DBP2PTradeEvent]) -> Optional[TradeEvent]:
        if rec is None:
            return None
        else:
            return TradeEvent(
                uid=rec.uid,
                adv_uid=rec.advert_uid,
                advertiser_uid=rec.advertiser_uid,
                position=rec.position,
                counterparty_position=rec.counterparty_position,
                quantity=rec.quantity,
                price=rec.price,
                epoch=rec.epoch,
                pay_methods=rec.pay_methods,
                fiat=rec.fiat,
                token=rec.token,
                side=rec.side,
                extra=rec.extra,
                ex=rec.exchange_code,
                usdt_price=rec.usdt_price,
                forex_ratio=rec.forex_ratio
            )

    @classmethod
    def __adv_get_pos(cls, rec: DBP2PAdvertPosition) -> AdvPosition:
        return AdvPosition(
            adv=cls.__adv_get_info(rec.adv),
            position=rec.position,
            epoch=rec.epoch
        )

    @classmethod
    def __adv_fetch(cls, filters: AdvFilters, reversed_: bool = False, limit: int = None) -> List[AdvertInfo]:
        queryset = DBP2PAdvertInfo.objects
        kwargs = {}
        distinct_data_hash = False
        if filters.uid:
            if isinstance(filters.uid, list):
                kwargs['uid__in'] = filters.uid
            else:
                kwargs['uid'] = filters.uid
        if filters.advertiser_uid:
            kwargs['advertiser__uid'] = filters.advertiser_uid
        if filters.advertiser_rating:
            kwargs['advertiser__rating__gte'] = filters.advertiser_rating
        if filters.advertiser_finish_rate:
            kwargs['advertiser__finish_rate__gte'] = filters.advertiser_finish_rate
        if filters.advertiser_total_trades:
            kwargs['advertiser__trades_total__gte'] = filters.advertiser_total_trades
        if filters.advertiser_trades:
            kwargs['advertiser__trades_month__gte'] = filters.advertiser_trades
        if filters.position:
            distinct_data_hash = True
            kwargs['positions__position'] = filters.position
        if filters.exchange_code:
            kwargs['advertiser__exchange_code'] = filters.exchange_code
        if filters.fiat:
            kwargs['fiat'] = filters.fiat
        if filters.side:
            kwargs['side'] = filters.side
        if kwargs:
            queryset = queryset.filter(**kwargs)
        if filters.epoch_eq or filters.epoch_gt or filters.epoch_lt or filters.epoch_from:
            if filters.epoch_eq:
                queryset = queryset.filter(
                    Q(epoch=filters.epoch_eq) | Q(positions__epoch=filters.epoch_eq)
                )
            if filters.epoch_gt:
                queryset = queryset.filter(positions__epoch__gte=filters.epoch_gt)
            if filters.epoch_lt:
                queryset = queryset.filter(positions__epoch__lte=filters.epoch_lt)
            if filters.epoch_from:
                queryset = queryset.filter(positions__epoch__gt=filters.epoch_from)
            distinct_data_hash = True
        if filters.quantity_gt:
            queryset = queryset.filter(quantity__gte=filters.quantity_gt)
        if filters.quantity_lt:
            queryset = queryset.filter(quantity__lte=filters.quantity_lt)
        if filters.pay_methods:
            queryset = queryset.filter(pay_methods__contains=list(filters.pay_methods))
        if filters.pay_methods_eq:
            queryset = queryset.filter(
                pay_methods_hash=cls.__hash_set(filters.pay_methods_eq)
            )
        if reversed_:
            order_by_fields = ['-epoch']
        else:
            order_by_fields = ['epoch']
        if distinct_data_hash:
            order_by_fields = ['data_hash'] + order_by_fields
        queryset = queryset.order_by(*order_by_fields)
        if distinct_data_hash:
            queryset = queryset.distinct('data_hash')
        if filters.last_only:
            queryset = queryset.order_by('uid', '-pk').distinct('uid')
        if limit is None:
            result = [cls.__adv_get_info(rec) for rec in queryset.all()]
        else:
            result = [cls.__adv_get_info(rec) for rec in queryset.all()[:limit]]
        return result

    @classmethod
    def __hash_set(cls, s: Set[str]) -> str:
        return hashlib.md5(
            json.dumps(sorted(s)).encode()
        ).hexdigest()

    @classmethod
    def __adv_append(
        cls, advertiser_uid: str, position: int,
        epoch: float, adv: AdvertInfo, compress: bool = False
    ) -> AdvPosition:
        with transaction.atomic():
            ret = cls.__adv_append_in_transaction(
                advertiser_uid, position, epoch, adv, compress
            )
            return ret

    @classmethod
    def __adv_append_multiple(cls, txns: List[AdvTransaction], compress: bool = False) -> List[AdvPosition]:
        with transaction.atomic():
            ret = []
            for txn in txns:
                pos = cls.__adv_append_in_transaction(
                    txn.advertiser_uid, txn.position,
                    txn.epoch, txn.adv, compress
                )
                ret.append(pos)
            return ret

    @classmethod
    def __trade_event_append_multiple(cls, exchange_code: str, events: List[TradeEvent]):
        with transaction.atomic():
            for e in events:
                DBP2PTradeEvent.objects.create(
                    uid=e.uid,
                    epoch=e.epoch,
                    exchange_code=exchange_code,
                    side=e.side,
                    token=e.token,
                    fiat=e.fiat,
                    advert_uid=e.adv_uid,
                    advertiser_uid=e.advertiser_uid,
                    position=e.position,
                    counterparty_position=e.counterparty_position,
                    price=e.price,
                    quantity=e.quantity,
                    extra=e.extra,
                    pay_methods=list(e.pay_methods),
                    pay_methods_hash=cls.__hash_set(e.pay_methods),
                    usdt_price=e.usdt_price,
                    forex_ratio=e.forex_ratio
                )

    @classmethod
    def __adv_append_in_transaction(
            cls, advertiser_uid: str, position: int,
            epoch: float, adv: AdvertInfo, compress: bool = False
    ) -> AdvPosition:
        rec_advertiser = DBP2PAdvertiser.objects.get(uid=advertiser_uid)
        cls.__ensure_epoch_metadata_stored(
            epoch=epoch, exchange_code=rec_advertiser.exchange_code,
            fiat=adv.fiat, side=adv.side, token=adv.token
        )
        last_rec = DBP2PAdvertInfo.objects.filter(uid=adv.uid, side=adv.side).last()
        if last_rec:
            if not last_rec.fiat:
                last_rec.fiat = adv.fiat
                last_rec.save()
            if not last_rec.token:
                last_rec.token = adv.token
                last_rec.save()
            if last_rec.data_hash == adv.data_hash():
                if epoch not in last_rec.duplicate_epochs:
                    if compress:
                        last_rec.duplicate_epochs = last_rec.duplicate_epochs[:-1] + [epoch]
                    else:
                        last_rec.duplicate_epochs.append(epoch)
                    last_rec.save()
            else:
                last_rec = None
        if last_rec is None:
            rec_info = DBP2PAdvertInfo.objects.create(
                advertiser=rec_advertiser,
                uid=adv.uid,
                price=adv.price,
                quantity=adv.quantity,
                quantity_fiat=adv.quantity_fiat,
                min_qty=adv.min_qty,
                max_qty=adv.max_qty,
                pay_methods=list(adv.pay_methods),
                epoch=epoch,
                duplicate_epochs=[],
                extra=adv.extra,
                data_hash=adv.data_hash(),
                exchange_code=rec_advertiser.exchange_code,
                pay_methods_hash=cls.__hash_set(adv.pay_methods),
                side=adv.side,
                fiat=adv.fiat,
                token=adv.token
            )
        else:
            rec_info = last_rec
        # build pos record
        rec_pos = DBP2PAdvertPosition.objects.create(
            adv=rec_info,
            position=position,
            epoch=epoch,
            fiat=adv.fiat,
            exchange_code=rec_advertiser.exchange_code,
            side=adv.side,
            token=adv.token
        )
        return cls.__adv_get_pos(rec_pos)

    @classmethod
    def __epoch_fetch(
        cls, exchange_code: str, side: str, token: str, fiat: str,
        offset: int = None, size: int = None, reverse: bool = False
    ) -> List[AdvEpochInfo]:
        query = DBP2PEpoch.objects.filter(
            exchange_code=exchange_code, side=side, token=token, fiat=fiat
        )
        if reverse:
            query = query.order_by('-pk')
        else:
            query = query.order_by('pk')
        result = []
        if size:
            if offset:
                end = offset + size
            else:
                end = size
        else:
            end = None
        for rec in query[offset:end]:
            result.append(
                AdvEpochInfo(
                    epoch=rec.epoch,
                    fiat=rec.fiat,
                    token=rec.token,
                    errors=rec.errors or {}
                )
            )
        return result

    @classmethod
    def __events_fetch(
        cls, filters: TradeEventFilters, reverse: bool = False,
        limit: int = None
    ) -> Tuple[List[TradeEvent], Optional[int]]:
        queryset = DBP2PTradeEvent.objects
        kwargs = {}
        if filters.pk_offset is not None:
            kwargs['pk__gt'] = filters.pk_offset
        if filters.adv_uid:
            kwargs['advert_uid'] = filters.adv_uid
        if filters.advertiser_uid:
            kwargs['advertiser_uid'] = filters.advertiser_uid
        if filters.epoch_from:
            kwargs['epoch__gt'] = filters.epoch_from
        if filters.exchange_code:
            kwargs['exchange_code'] = filters.exchange_code
        if filters.fiat:
            kwargs['fiat'] = filters.fiat
        if filters.token:
            kwargs['token'] = filters.token
        if kwargs:
            queryset = queryset.filter(**kwargs)
        if reverse:
            queryset = queryset.order_by('-pk')
        else:
            queryset = queryset.order_by('pk')
        if limit is None:
            recs = list(queryset.all())
        else:
            recs = list(queryset.all()[:limit])
        result = [cls.__event_extract(rec) for rec in recs]
        last_pk = max([rec.pk for rec in recs]) if recs else None
        return result, last_pk

    @classmethod
    @lru_cache(maxsize=128)
    def __ensure_epoch_metadata_stored(
        cls, epoch: float, exchange_code: str, fiat: str,
        side: str, token: str, errors: Dict = None
    ) -> DBP2PEpoch:
        rec, _ = DBP2PEpoch.objects.get_or_create(
            defaults=dict(errors=errors or {}),
            epoch=epoch, exchange_code=exchange_code,
            side=side, token=token, fiat=fiat
        )
        return rec

    @classmethod
    def __pos_fetch(
        cls, epoch: float, exchange_code: str, side: str, token: str, fiat: str
    ) -> List[AdvPosition]:
        query = DBP2PAdvertPosition.objects.filter(
            epoch=epoch, exchange_code=exchange_code,
            side=side, token=token, fiat=fiat
        ).order_by('position')
        result = []
        for rec in query.all():
            pos = AdvPosition(
                epoch=epoch,
                position=rec.position,
                adv=cls.__adv_get_info(rec.adv)
            )
            result.append(pos)
        return result


class P2PAdvertsHeap:

    def __init__(
        self, exchange_code: str, token: str, fiat: str, crud: CRUDAdverts,
        lru_size: int = 1000
    ):
        self.exchange_code = exchange_code
        self.token = token
        self.fiat = fiat
        self._lru = LRUCache(capacity=lru_size)
        self._crud = crud

    async def refresh(self, infos: List[AdvertInfo]):
        for info in infos:
            self._lru.put(key=info.uid, value=info)

    async def get_one(self, uid: str) -> Optional[AdvertInfo]:
        res = await self.load([uid])
        return res[0] if res else None

    async def load(self, uids: List[str]) -> List[AdvertInfo]:
        result = []
        uid_from_db = []
        for uid in list(set(uids)):
            info = self._lru.get(uid)
            if info is not None:
                result.append(info)
            else:
                uid_from_db.append(uid)
        if uid_from_db:
            infos = await self._crud.adv_fetch(
                filters=CRUDAdverts.AdvFilters(
                    uid=uid_from_db,
                    exchange_code=self.exchange_code,
                    fiat=self.fiat,
                    token=self.token,
                    last_only=True
                )
            )
            for info in infos:
                self._lru.put(key=info.uid, value=info)
            result.extend(infos)
        return result
