DEF_BOOKS_DEPTH_LEVEL = 20
DEF_TICKERS_SPEED_MS = 1000
PERPETUAL_TOKENS = ['USDT', 'USDC', 'DAI']
KLINE_WINDOW_SECS = 60*60  # 1 hr


def build_any_key(exchange_code: str, *items) -> str:
    items = [item.replace('/', '-') for item in items]
    return '/'.join([exchange_code] + list(items))


def build_tickers_key(exchange_code: str) -> str:
    return build_any_key(exchange_code, 'tickers')


def build_book_ticker_key(exchange_code: str, symbol: str, section: str = 'spot') -> str:
    return build_any_key(exchange_code, section, 'book_ticker', symbol)


def build_book_depth_key(exchange_code: str, symbol: str, section: str = 'spot') -> str:
    return build_any_key(exchange_code, section, 'book_depth', symbol)


def build_candles_key(exchange_code: str, symbol: str, section: str = 'spot') -> str:
    return build_any_key(exchange_code, section, 'candles', symbol)
