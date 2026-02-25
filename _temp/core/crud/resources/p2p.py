import datetime
import json
from hashlib import md5
from typing import List, Set, Dict, Optional, Literal, Tuple

from pydantic import BaseModel, computed_field

from core.p2p import TradeSide
from core.utils import float_to_datetime


class Advertiser(BaseModel):
    exchange_code: str
    uid: str
    nick: str
    rating: Optional[float] = None
    finish_rate: Optional[float] = None
    trades_month: Optional[int] = None
    trades_total: Optional[int] = None
    extra: Optional[dict] = None
    verified_merchant: Optional[bool] = None
    recommended: Optional[bool] = False

    def data_hash(self) -> str:
        return md5(
            json.dumps(
                self.model_dump(mode='json'),
                sort_keys=True
            ).encode()
        ).hexdigest()


class AdvertInfo(BaseModel):
    uid: str
    advertiser: Advertiser = None
    price: float
    quantity: float
    quantity_fiat: Optional[float] = None
    min_qty: float
    max_qty: float
    pay_methods: Set[str]
    epoch: float
    duplicate_epochs: List[float] = None
    extra: Optional[Dict] = None
    fiat: Optional[str] = None
    side: Optional[Literal[TradeSide.BUY, TradeSide.SELL]] = None
    token: Optional[str] = None

    def data_hash(self) -> str:
        return md5(
            json.dumps(
                self.model_dump(
                    mode='json',
                    exclude={'duplicate_epochs', 'epoch', 'advertiser'}
                ),
                sort_keys=True
            ).encode()
        ).hexdigest()


class AdvPosition(BaseModel):
    adv: AdvertInfo
    position: int
    epoch: float


class AdvEpochInfo(BaseModel):
    epoch: float
    fiat: str
    token: str
    errors: Dict = {}


class CupStatItem(BaseModel):
    """Элемент стакана заявок"""
    adv_uid: str
    index: int
    price: float
    quantity: float
    min_qty: float
    max_qty: float
    pay_methods: Set[str]
    advertiser: Optional[Advertiser] = None
    extra: dict = {}


class CupStat(BaseModel):
    epoch: float
    fiat: str
    token: str
    utc: float
    buy: List[CupStatItem] = []
    sell: List[CupStatItem] = []

    def append(self, pos: AdvPosition):
        if pos.adv.side == TradeSide.SELL:
            container = self.sell
        else:
            container = self.buy
        container.append(
            CupStatItem(
                adv_uid=pos.adv.uid,
                index=pos.position,
                price=pos.adv.price,
                quantity=pos.adv.quantity,
                min_qty=pos.adv.min_qty,
                max_qty=pos.adv.max_qty,
                pay_methods=set(pos.adv.pay_methods),
                advertiser=pos.adv.advertiser
            )
        )

    def filter(self, payment_methods: Set[str], a: Optional[Advertiser] = None) -> 'CupStat':
        other = CupStat(
            epoch=self.epoch, fiat=self.fiat,
            token=self.token, utc=self.utc
        )
        buy_pos = 0
        for my_item in self.buy:
            if self.exact(my_item, payment_methods, a):
                buy_pos += 1
                their_item = my_item.model_copy()
                their_item.index = buy_pos
                other.buy.append(their_item)
        sell_pos = 0
        for my_item in self.sell:
            if self.exact(my_item, payment_methods, a):
                sell_pos += 1
                their_item = my_item.model_copy()
                their_item.index = sell_pos
                other.sell.append(their_item)
        return other

    def filter_with_good_advertisers(self):

        def __filter(items: List[CupStatItem]) -> List[CupStatItem]:
            res = []
            for item in items:
                if item.advertiser and item.advertiser.finish_rate > 85.0:
                    res.append(item)
            return res

        __filter(self.buy)
        __filter(self.sell)

    def find(self, adv_uid: str) -> Tuple[Optional[CupStatItem], Optional[TradeSide]]:
        for sell_item in self.sell:
            if sell_item.adv_uid == adv_uid:
                return sell_item, TradeSide.SELL
        for buy_item in self.buy:
            if buy_item.adv_uid == adv_uid:
                return buy_item, TradeSide.BUY
        return None, None

    def to_dict(self) -> dict:
        self.buy = sorted(self.buy, key=lambda item: item.index)
        self.sell = sorted(self.sell, key=lambda item: item.index)
        return self.model_dump(mode='json')

    @staticmethod
    def exact(
        item: CupStatItem,
        payment_methods: Set[str], a: Optional[Advertiser] = None
    ) -> bool:
        if payment_methods.intersection(item.pay_methods):
            if a and item.advertiser:
                if a.verified_merchant == item.advertiser.verified_merchant \
                        and a.recommended == item.advertiser.recommended:
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False


class TradeEvent(BaseModel):
    uid: Optional[str] = None
    adv_uid: str
    advertiser_uid: str
    position: int
    counterparty_position: Optional[int] = None
    quantity: float
    price: float
    epoch: float
    pay_methods: Set[str]
    fiat: str = None
    token: str = None
    side: str = None
    extra: Optional[Dict] = None
    ex: Optional[str] = None
    usdt_price: Optional[float] = None
    forex_ratio: Optional[float] = None

    @computed_field
    @property
    def amount(self) -> float:
        return self.price * self.quantity

    @property
    def advertiser(self) -> Optional[Advertiser]:
        if self.extra:
            js = self.extra.get('advertiser', None)
            if js:
                return Advertiser(**js)
            else:
                return None
        else:
            return None

    @computed_field
    @property
    def utc(self) -> datetime.datetime:
        return float_to_datetime(self.epoch)


class TradeAdvertAccum(BaseModel):
    trades: int = 0
    total_qty: float = 0.0
    total_money: float = 0.0
    last_epoch: Optional[float] = None
