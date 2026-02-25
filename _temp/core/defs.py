from typing import Union, List, Dict
from dataclasses import dataclass, is_dataclass
from dataclasses_json import dataclass_json

from .exceptions import SerializationError


@dataclass_json
class JsonSerializable:

    def as_json(self) -> Dict:
        self.__validate_dataclass()
        return self.to_dict()

    @classmethod
    def from_json_ext(cls, js: Dict) -> "JsonSerializable":
        cls.__validate_dataclass()
        return cls.schema().load(js)

    @classmethod
    def __validate_dataclass(cls):
        if not is_dataclass(cls):
            raise RuntimeError('Instance should be instance of dataclass')


@dataclass
class CurrencyPair(JsonSerializable):
    # base currency, ex: EUR/USD  EUR - is base currency
    base: str
    # counter currency, ex: EUR/USD  USD - is quota currency
    quota: str  # TODO: replace with "quote"
    # ratio price, ex: EUR/USD=1.3045 here 1.3045 U.S. dollars is ratio price
    ratio: float
    # last update utc timestamp
    utc: Union[None, float] = None

    @property
    def code(self) -> str:
        return self.build_code(self.base, self.quota)

    @staticmethod
    def build_code(base: str, quota: str) -> str:
        return f'{base}/{quota}'

    def to_str(self) -> str:
        return f'{self.code}={self.ratio}'

    @staticmethod
    def from_str(s: str) -> "CurrencyPair":
        if s.count('/') == 1 and s.count('=') == 1:
            pair, ratio = s.split('=')
            base, quota = pair.split('/')
            return CurrencyPair(base=base, quota=quota, ratio=float(ratio), utc=None)
        else:
            raise SerializationError('Invalid string format')

    def copy(self) -> "CurrencyPair":
        new = CurrencyPair(base=self.base, quota=self.quota, ratio=self.ratio, utc=self.utc)
        return new

    def reversed(self) -> "CurrencyPair":
        new = CurrencyPair(base=self.quota, quota=self.base, ratio=1/self.ratio, utc=self.utc)
        return new


@dataclass
class Triangle:
    pair1: CurrencyPair
    pair2: CurrencyPair
    pair3: CurrencyPair

    @property
    def codes(self) -> List[str]:
        return [cur.code for cur in self.trades]

    @property
    def trades(self) -> List[CurrencyPair]:
        return [self.pair1, self.pair2, self.pair3]

    def reversed(self) -> "Triangle":
        return Triangle(pair1=self.pair3.reversed(), pair2=self.pair2.reversed(), pair3=self.pair1.reversed())

    @property
    def ratio(self) -> float:
        ratio = self.pair1.ratio * self.pair2.ratio * self.pair3.ratio
        return ratio


@dataclass
class Exchanges:
    # Unique code of exchange table
    code: str
    # currency pairs
    pairs: List[CurrencyPair]


@dataclass
class BookTickerStat:
    # currency pair code
    code: str
    # bid/ask prices
    bid_price: float
    ask_price: float
    # qty total (fetched from order books)
    bid_qty_total: float
    ask_qty_total: float
    # last update utc timestamp
    utc: Union[None, float]
