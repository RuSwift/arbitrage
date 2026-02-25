import random
import enum
from abc import abstractmethod
from typing import List, Tuple, Optional
from contextlib import asynccontextmanager
from dataclasses import dataclass

from django.conf import settings

from core.http import AnonymousWebBrowser, TorCommander, BaseSocksCommander
from core.exceptions import BrowserConnectionError


@dataclass(frozen=True)
class FiatInfo:
    code: str
    country_code: Optional[str] = None
    symbol: Optional[str] = None


@dataclass(frozen=True)
class TokenInfo:
    code: str


@dataclass(frozen=True)
class PaymentMethod:
    code: str
    label: str = None
    extra: dict = None


class TradeSide(str, enum.Enum):
    BUY = 'BUY'
    SELL = 'SELL'


@dataclass(frozen=True)
class P2PAdvertInfo:
    uid: str
    price: float
    quantity: float
    min_max_qty: Tuple[float, float]
    pay_methods: List[str]
    extra: dict = None


@dataclass(frozen=True)
class P2PAdvertiser:
    uid: str
    nick: str
    rating: float = None
    finish_rate: float = None
    trades_month: int = None
    trades_total: int = None
    extra: dict = None
    verified_merchant: Optional[bool] = None
    recommended: Optional[bool] = False


@dataclass(frozen=True)
class P2PAdvert:
    info: P2PAdvertInfo
    advertiser: P2PAdvertiser
    utc: float


TYPE_SOCKS_ADDR = Tuple[str, int]
NO_SOCKS: TYPE_SOCKS_ADDR = ('*', 0)


class P2PService:

    def __init__(self, socks_addr: TYPE_SOCKS_ADDR = None, is_tor: bool = True):
        self.__socks: List[Optional[TYPE_SOCKS_ADDR]] = []
        self.cmd: Optional[TorCommander] = None
        if socks_addr:
            if socks_addr != NO_SOCKS:
                host, port = socks_addr
                self.__socks.append(
                    (str(host), int(port))
                )
                if is_tor:
                    self.cmd = TorCommander(
                        host=str(host), port=int(port)
                    )
                else:
                    self.cmd = BaseSocksCommander(
                        host=str(host), port=int(port)
                    )
        else:
            for item in settings.TOR_SOCKS_PROXY:
                host, port = item.host, item.port
                self.__socks.append(
                    (str(host), int(port))
                )
        random.shuffle(self.__socks)  # randomize
        self.__socks.append(None)  # Режим без TOR-Socks всегда будет доступен
        self._session: Optional[AnonymousWebBrowser] = None

    @abstractmethod
    async def _create_session(
        self, socks: Optional[Tuple[str, int]]
    ) -> AnonymousWebBrowser:
        pass

    @asynccontextmanager
    async def session(self, mono: bool = False, test_headers: bool = True) -> AnonymousWebBrowser:
        if self._session is None:
            for sock in self.__socks:
                try:
                    self._session = await self._create_session(sock)
                    if sock:
                        self.cmd = TorCommander(
                            host=str(sock[0]), port=int(sock[1])
                        )
                    else:
                        self.cmd = None
                except BrowserConnectionError as e:
                    self._session = None
                    continue
                else:
                    break
            await self._session.open_session(test_headers=test_headers)
        try:
            yield self._session
        finally:
            if mono:
                s = self._session
                self._session = None
                await s.close_session()

    async def terminate(self):
        if self._session:
            await self._session.close_session()
            self._session = None
            self.cmd = None

    @abstractmethod
    async def load_fiat_list(self) -> List[FiatInfo]:
        pass

    @abstractmethod
    async def load_tokens(self, fiat: str) -> List[TokenInfo]:
        pass

    @abstractmethod
    async def load_payment_methods(self, fiat: str) -> List[PaymentMethod]:
        pass

    @abstractmethod
    async def load_adv_page(
        self, side: TradeSide, fiat: str, token: str,
        page: int, size: int = None
    ) -> List[P2PAdvert]:
        pass
