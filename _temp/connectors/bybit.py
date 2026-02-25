import json
from typing import List, Optional, Dict, Tuple, Type

from django.conf import settings
from pybit.exceptions import InvalidRequestError
from pybit.unified_trading import MarketHTTP, WebSocket

import aioredis
import core.throttler
from core.defs import CurrencyPair
from core.http import AnonymousWebBrowser
from core.utils import utc_now_float, float_to_datetime
from core.exceptions import BrowserOperationError
from core.connectors import (
    BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick
)
from core.p2p import (
    P2PService, FiatInfo, TokenInfo, PaymentMethod,
    TradeSide, P2PAdvert, P2PAdvertInfo, P2PAdvertiser, NO_SOCKS
)

from .utils import redis_get, redis_set, build_tickers_dict
from .defs import PERPETUAL_TOKENS, KLINE_WINDOW_SECS


class LocalCache:

    REDIS_KEY_PREFIX = 'p2p/bybit/cache/94b6eb3c899d4ae4a00ce810d9b11cd4/'

    def __init__(self, throttle_timeout: float):
        self.__throttle_timeout = throttle_timeout

    async def get(self, name: str) -> Optional[Dict]:
        redis_key = self.REDIS_KEY_PREFIX + name
        redis = aioredis.Redis(
            connection_pool=aioredis.ConnectionPool.from_url(
                settings.REDIS_DSN
            )
        )
        raw = await redis.get(redis_key)
        if raw is None:
            return None
        else:
            return json.loads(raw)

    async def set(self, name: str, value: Dict):
        redis_key = self.REDIS_KEY_PREFIX + name
        redis = aioredis.Redis(
            connection_pool=aioredis.ConnectionPool.from_url(
                settings.REDIS_DSN
            )
        )
        await redis.set(
            redis_key, json.dumps(value),
            ex=int(self.__throttle_timeout)
        )


class ByBitP2PService(P2PService):

    LOAD_ADV_SIZE = 1000
    _cache = LocalCache(throttle_timeout=60*60)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__sell: List[P2PAdvert] = []
        self.__buy: List[P2PAdvert] = []
        self.__adv_fiat: Optional[str] = None

    async def _create_session(self, socks: Optional[Tuple[str, int]]) -> AnonymousWebBrowser:
        return AnonymousWebBrowser(
            base_url='https://api2.bybit.com',
            lib=AnonymousWebBrowser.IOLib.AIOHTTP,
            socks=socks,
            default_headers={
                'Accept': 'application/json',
                'Accept-Language': 'ru-RU',
                # 'Content-Type': 'application/x-www-form-urlencoded'
            }
        )

    async def load_fiat_list(self) -> List[FiatInfo]:
        initial = await self.__get_initial()
        currency_set = set()
        for item in initial.get('currencies', []):
            currency_set.update({item['currencyId']})
        return [FiatInfo(code=cur) for cur in currency_set]

    async def load_tokens(self, fiat: str) -> List[TokenInfo]:
        initial = await self.__get_initial()
        symbols = filter(lambda x: x['currencyId'] == fiat, initial['symbols'])
        return [TokenInfo(code=sym['tokenId']) for sym in symbols]

    async def load_payment_methods(self, fiat: str) -> List[PaymentMethod]:
        payments = await self.__get_payments()
        fiat_map = json.loads(payments['currencyPaymentIdMap']).get(fiat, [])
        fiat_map = {str(i) for i in fiat_map}
        methods: Dict[str, PaymentMethod] = {
            item['paymentType']:
            PaymentMethod(
                code=str(item['paymentType']),
                label=item['paymentName']
            )
            for item in payments.get('paymentConfigVo', [])
        }
        return [m for code, m in methods.items() if code in fiat_map]

    async def load_adv_page(
        self, side: TradeSide, fiat: str,
        token: str, page: int, size: int = None
    ) -> List[P2PAdvert]:
        if fiat != self.__adv_fiat:
            self.__sell.clear()
            self.__buy.clear()
            self.__adv_fiat = fiat
        if side == TradeSide.SELL:
            container = self.__sell
        else:
            container = self.__buy
        make_request = False
        if not container:
            make_request = True
        if make_request:
            await self.__load_adv(container, side, token, fiat)
        if size is None:
            size = 10
        offset = (page-1)*size
        return container[offset:offset+size]

    async def __load_adv(
        self, container: List[P2PAdvert], side: TradeSide,
        token: str, fiat: str
    ):
        payments = await self.load_payment_methods(fiat)
        async with self.session(test_headers=False, mono=True) as browser:
            # Load app data
            container.clear()
            js = await browser.post(
                path='/fiat/otc/item/online',
                js={
                    "userId": "",
                    "tokenId": token,
                    "currencyId": fiat,
                    "payment": [],
                    "side": "1" if side == TradeSide.SELL else "0",
                    "size": f"{self.LOAD_ADV_SIZE}",
                    "page": "1",
                    "amount": "",
                    "authMaker": False,
                    "canTrade": False
                }
            )
            if js['ret_code'] == 0:
                pay_as_dict = {p.code: p.label for p in payments}
                for item in js['result']['items']:
                    pay_methods = [
                        pay_as_dict.get(i) for i in item['payments']
                    ]
                    pay_methods = [m for m in pay_methods if m]
                    price = float(item['price'])
                    qty_min = float(item['minAmount']) / price
                    qty_max = float(item['maxAmount']) / price
                    info = P2PAdvertInfo(
                        uid=item['id'],
                        price=price,
                        quantity=float(item['lastQuantity']),
                        min_max_qty=(qty_min, qty_max),
                        pay_methods=pay_methods,
                        extra=item
                    )
                    advertiser = P2PAdvertiser(
                        uid=item['accountId'],
                        nick=item['nickName'],
                        rating=float(item['recentExecuteRate']),
                        finish_rate=float(item['recentExecuteRate']),
                        trades_month=int(item['recentOrderNum']),
                        trades_total=int(item['finishNum']),
                        extra=None,
                        verified_merchant=item['recommend'] or (item['authStatus'] < 2),
                        recommended=item['recommend']
                    )
                    adv = P2PAdvert(
                        info=info,
                        advertiser=advertiser,
                        utc=utc_now_float()
                    )
                    container.append(adv)
            else:
                raise BrowserOperationError('Response error!')

    async def __get_initial(self) -> Dict:
        cached = await self._cache.get('initial')
        if not cached:
            async with self.session(test_headers=False, mono=True) as browser:
                # Load app data
                raw = await browser.get('/fiat/p2p/config/initial')
                js = json.loads(raw)
                if js['ret_code'] == 0:
                    value = js['result']
                    await self._cache.set('initial', value)
                    return value
                else:
                    raise BrowserOperationError(
                        js.get('ret_msg', 'Response error!'))
        else:
            return cached

    async def __get_payments(self) -> Dict:
        cached = await self._cache.get('payments')
        if not cached:
            async with self.session(test_headers=False, mono=True) as browser:
                # Load app data
                js = await browser.post('/fiat/otc/configuration/queryAllPaymentList')
                if js['ret_code'] == 0:
                    value = js['result']
                    await self._cache.set('payments', value)
                    return value
                else:
                    raise BrowserOperationError(js.get('ret_msg', 'Response error!'))
        else:
            return cached


class ByBitConnector(BaseExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)
        self.__cached_tickers: Optional[List[Ticker]] = None
        self.__cached_tickers_dict: Dict[str, Ticker] = {}
        self.__public_market_api = MarketHTTP(testnet=is_testing)
        self.__public_ws: Optional[WebSocket] = None
        self.__cb: Optional[BaseExchangeConnector.Callback] = None
        self.__redis_cache_key = f'connector/{self.exchange_id()}/cached'

    @classmethod
    def exchange_id(cls) -> str:
        return 'bybit'

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        if self.__public_ws is not None:
            raise RuntimeError('Websocket Stream API already active. Close resources at first!')
        self.__public_ws = WebSocket(channel_type='spot', testnet=False)
        is_conn = self.__public_ws.is_connected()
        if is_conn:
            if not self.__cached_tickers_dict:
                self.get_all_tickers()
            if symbols is None:
                filtering_symbols = [sym.exchange_symbol for sym in self.__cached_tickers]
            else:
                filtering_symbols = [sym.exchange_symbol for sym in self.__cached_tickers if sym.symbol in symbols]
            self.__cb = cb
            for sym in filtering_symbols:
                self.__public_ws.orderbook_stream(depth=1, symbol=sym, callback=self.__handle_ws_message)
                if depth:
                    self.__public_ws.orderbook_stream(depth=50, symbol=sym, callback=self.__handle_ws_message)
        else:
            raise RuntimeError('Public websocket stream API connection error!')

    def stop(self):
        self.__public_ws.ws.close()
        self.__public_ws = None
        self.__cb = None

    def get_all_tickers(self) -> List[Ticker]:
        if self.__cached_tickers:
            return self.__cached_tickers
        cached_tickers = redis_get(self.__redis_cache_key)
        if cached_tickers:
            self.__cached_tickers = [Ticker.from_json(json.dumps(tk)) for tk in cached_tickers]
            self.__cached_tickers_dict = build_tickers_dict(self.__cached_tickers)
            return self.__cached_tickers

        tickers = []
        infos = self.__public_market_api.get_instruments_info(category='spot', status='Trading')
        if infos['retCode'] == 0:
            symbols = infos['result']['list']
            for symbol in symbols:
                tickers.append(
                    Ticker(
                        symbol=CurrencyPair.build_code(base=symbol['baseCoin'], quota=symbol['quoteCoin']),
                        base=symbol['baseCoin'],
                        quote=symbol['quoteCoin'],
                        is_spot_enabled=True,

                        # TODO: потом надо будет допилить детект чтобы понимать можно ли хеджировать позицию
                        is_margin_enabled=False,
                        exchange_symbol=symbol['symbol']
                    )
                )
            self.__cached_tickers = tickers
            self.__cached_tickers_dict = build_tickers_dict(tickers)
            redis_set(
                self.__redis_cache_key,
                [tk.as_json() for tk in tickers],
                ttl=settings.EXCHANGE_CONNECTOR_CACHE_TIMEOUT
            )
            return tickers
        else:
            raise RuntimeError(infos.get('retMsg', 'Error while retrieve tickers'))

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        symbol = pair_code.replace('/', '')
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        ticker: Ticker = self.__cached_tickers_dict.get(symbol)
        if not ticker:
            return None
        try:
            resp = self.__public_market_api.get_tickers(
                category='spot', symbol=symbol
            )
        except InvalidRequestError:
            return None
        if resp.get('retCode') == 0:
            record = resp['result']['list'][0]
            return CurrencyPair(
                base=ticker.base,
                quota=ticker.quote,
                ratio=float(record['lastPrice']),
                utc=utc_now_float()
            )
        else:
            return None

    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        if symbols is None:
            if not self.__cached_tickers_dict:
                self.get_all_tickers()
            filtering_symbols = set(self.__cached_tickers_dict.keys())
        else:
            filtering_symbols = set([sym.replace('/', '') for sym in symbols])
        resp = self.__public_market_api.get_tickers(category='spot')
        if resp['retCode'] == 0:
            pairs = []
            prices = resp.get('result', {}).get('list', [])
            for p in prices:
                if p['symbol'] in filtering_symbols:
                    ticker: Ticker = self.__cached_tickers_dict.get(p['symbol'])
                    if ticker:
                        pairs.append(
                            CurrencyPair(
                                base=ticker.base,
                                quota=ticker.quote,
                                ratio=float(p['lastPrice']),
                                utc=utc_now_float()
                            )
                        )
            return pairs
        else:
            raise RuntimeError(resp.get('retMsg', 'Error while retrieve tickers prices'))

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        ex_sym = self.__get_exchange_symbol(symbol)
        if ex_sym:
            resp = self.__public_market_api.get_orderbook(
                category='spot', symbol=ex_sym, limit=limit
            )
            if resp['retCode'] == 0:
                data = resp['result']
                _, depth = self.__raw_data_to_events(data)
                return depth
            else:
                raise RuntimeError(
                    resp.get('retMsg', 'Error while retrieve order book'))
        else:
            return None

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        ex_sym = self.__get_exchange_symbol(symbol)
        if ex_sym:
            resp = self.__public_market_api.get_kline(
                category='spot', symbol=ex_sym, interval='1',
                limit=KLINE_WINDOW_SECS // 60
            )
            if resp['retCode'] == 0:
                items = resp['result']['list']
                klines = [
                    self.__deserialize_candle_stick(symbol, i) for i in items
                ]
                return klines
            else:
                raise RuntimeError(
                    resp.get('retMsg', 'Error while retrieve klines')
                )
        else:
            return None

    @classmethod
    def p2p(cls) -> Optional[Type[P2PService]]:
        return ByBitP2PService

    def __get_exchange_symbol(self, symbol: str) -> Optional[str]:
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        ticker = self.__cached_tickers_dict.get(symbol)
        if ticker:
            return ticker.exchange_symbol
        else:
            return None

    def __handle_ws_message(self, message: Dict):
        if self.__cb:
            topic = message.get('topic')
            stream = '.'.join(topic.split('.')[:-1])
            if stream.startswith('orderbook.'):
                _, depth = stream.split('.')
                book_event, depth_event = self.__raw_data_to_events(message['data'])
                if depth == '1':
                    # для depth == 1 биржа присылает depth оч грубо
                    depth_event = None
                else:
                    # для depth > 1 биржа присылает тикеры неправильно
                    book_event = None
                if book_event and not self._throttler.may_pass(book_event.symbol, tag='book'):
                    book_event = None
                if depth_event and not self._throttler.may_pass(depth_event.symbol, tag='depth'):
                    depth_event = None
                if book_event or depth_event:
                    self.__cb.handle(book=book_event, depth=depth_event)

    def __raw_data_to_events(self, data: Dict) -> (Optional[BookTicker], Optional[BookDepth]):
        ticker: Ticker = self.__cached_tickers_dict.get(data['s'])
        # Ticker
        bid_price_str, bid_qty_str = data['b'][0]
        ask_price_str, ask_qty_str = data['a'][0]
        book = BookTicker(
            symbol=ticker.symbol,
            bid_price=float(bid_price_str),
            bid_qty=float(bid_qty_str),
            ask_price=float(ask_price_str),
            ask_qty=float(ask_qty_str),
            last_update_id=data['u'],
            utc=utc_now_float()
        )
        # Depth
        bids = []
        asks = []
        for item in data['b']:
            price, qty = item
            bids.append(BidAsk(price=float(price), quantity=float(qty)))
        for item in data['a']:
            price, qty = item
            asks.append(BidAsk(price=float(price), quantity=float(qty)))
        depth = BookDepth(
            symbol=ticker.symbol,
            exchange_symbol=ticker.exchange_symbol,
            bids=bids,
            asks=asks,
            last_update_id=data['u'],
            utc=utc_now_float()
        )
        return book, depth

    @classmethod
    def __deserialize_candle_stick(cls, symbol: str, data: List) -> CandleStick:
        base, quote = symbol.split('/')
        quote_volume = float(data[6])
        if quote in PERPETUAL_TOKENS:
            usd_volume = quote_volume
        else:
            usd_volume = None
        return CandleStick(
            utc_open_time=float(data[0]) / 1000,
            open_price=float(data[1]),
            high_price=float(data[2]),
            low_price=float(data[3]),
            close_price=float(data[4]),
            coin_volume=float(data[5]),
            usd_volume=usd_volume
        )
