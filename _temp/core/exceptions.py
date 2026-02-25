class BaseArbitrageError(Exception):
    pass


class SerializationError(BaseArbitrageError):
    pass


class UnknownCurrencyPair(BaseArbitrageError):
    pass


class ExchangeResponseError(BaseArbitrageError):
    pass


class BrowserConnectionError(BaseArbitrageError):
    pass


class BrowserOperationError(BaseArbitrageError):

    def __init__(self, detail: str = None, error_code: int = None):
        super().__init__(detail)
        self.error_code = error_code
        self.detail = detail


class HTMLParserError(BaseArbitrageError):
    pass


class StatCollectorError(BaseArbitrageError):
    pass


class URLAccessError(BaseArbitrageError):
    pass
