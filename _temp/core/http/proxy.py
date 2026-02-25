import socket
from enum import IntEnum
from time import sleep
from urllib.parse import urljoin
from typing import Tuple, Optional, Mapping, Any

import stem
import stem.control
import aiohttp
import fake_useragent
import requests
from aiohttp_socks import ProxyType, ProxyConnector
from asgiref.sync import sync_to_async

from core.exceptions import BrowserConnectionError, BrowserOperationError


class AnonymousWebBrowser:
    """Легкая эмуляция броузера
    более честная эмуляция возможна через Selenium driver
    """

    class IOLib(IntEnum):
        AIOHTTP = 0
        REQUESTS = 1
        DEFAULT = REQUESTS

    def __init__(
        self, base_url: str,
        socks: Tuple[str, int] = None,
        lib: IOLib = IOLib.DEFAULT,
        default_headers: Mapping = None
    ):
        self._lib = lib
        self.__ua = fake_useragent.UserAgent()
        self.__socks = socks
        self.__base_url = base_url
        self.__default_headers = default_headers
        if self._lib == self.IOLib.AIOHTTP:
            if socks:
                host, port = socks
                self.__connector = ProxyConnector(
                    proxy_type=ProxyType.SOCKS5,
                    host=host,
                    port=port,
                    # username='user',
                    # password='password',
                    rdns=True
                )
            else:
                self.__connector = aiohttp.TCPConnector(
                    verify_ssl=True, keepalive_timeout=360
                )
            self.__client: Optional[aiohttp.ClientSession] = None
        elif self._lib == self.IOLib.REQUESTS:
            self.__requests_client: Optional[requests.Session] = None
        else:
            raise AssertionError('Unsupported Lib')

    @property
    def headers(self) -> Optional[Mapping]:
        headers = None
        if self._lib == self.IOLib.AIOHTTP:
            if self.__client:
                headers = self.__client.headers
        elif self._lib == self.IOLib.REQUESTS:
            if self.__requests_client:
                headers = dict(self.__requests_client.headers)
        return headers

    @property
    def cookies(self) -> Optional[Mapping]:
        cookies = None
        if self._lib == self.IOLib.AIOHTTP:
            if self.__client:
                cookies = dict(self.__client.cookie_jar)
        elif self._lib == self.IOLib.REQUESTS:
            if self.__requests_client:
                cookies = dict(self.__requests_client.cookies)
        return cookies

    async def open_session(self, test_headers: bool = True):
        if self._lib == self.IOLib.AIOHTTP:
            headers = {
                'User-Agent': self.__ua.chrome
            }
            if self.__default_headers:
                headers.update(self.__default_headers)
            self.__client = aiohttp.ClientSession(
                connector=self.__connector,
                base_url=self.__base_url,
                connector_owner=True,
                headers=headers
            )
            await self.__client.__aenter__()
            if test_headers:
                ok = await self.test_headers(raise_error=False)
                if not ok:
                    await self.close_session()
                    raise BrowserConnectionError
        elif self._lib == self.IOLib.REQUESTS:
            self.__requests_client = requests.Session()
            self.__requests_client.headers.update(
                {'User-Agent': self.__ua.chrome}
            )
            if self.__socks:
                host, port = self.__socks
                self.__requests_client.proxies = {
                    'http': f'socks5://{host}:{port}'
                }

    async def close_session(self):
        if self._lib == self.IOLib.AIOHTTP:
            await self.__client.__aexit__(None, None, None)
            self.__client = None
        elif self._lib == self.IOLib.REQUESTS:
            await sync_to_async(self.__requests_client.close)()
            self.__requests_client = None

    async def test_headers(
        self, path: str = '/', raise_error: bool = True
    ) -> bool:
        if self._lib == self.IOLib.AIOHTTP:
            resp = await self.__client.head(url=path, allow_redirects=True)
            ok = 200 <= resp.status < 300
        elif self._lib == self.IOLib.REQUESTS:
            resp = await sync_to_async(
                self.__requests_client.head
            )(
                url=urljoin(self.__base_url, path), allow_redirects=True
            )
            ok = 200 <= resp.status < 300
        else:
            ok = False
        if not ok:
            if raise_error:
                raise BrowserConnectionError('Test headers error!')
            else:
                return False
        else:
            return True

    async def get(self, path: str) -> str:
        if self._lib == self.IOLib.AIOHTTP:
            resp = await self.__client.get(
                path, allow_redirects=True
            )
            if resp.status == 200:
                html = await resp.text()
                return html
            else:
                msg = await resp.text()
                raise BrowserOperationError(msg)
        elif self._lib == self.IOLib.REQUESTS:
            url = urljoin(self.__base_url, path)
            resp = await sync_to_async(self.__requests_client.get)(
                url, allow_redirects=True
            )
            if resp.status_code == 200:
                html = resp.text
                return html
            else:
                raise BrowserOperationError(resp.text)

    async def post(self, path: str, js: Optional[dict] = None) -> Any:
        if self._lib == self.IOLib.AIOHTTP:
            resp = await self.__client.post(
                path, json=js
            )
            if resp.status == 200:
                ret = await resp.json()
                return ret
            else:
                msg = await resp.text()
                raise BrowserOperationError(msg)
        elif self._lib == self.IOLib.REQUESTS:
            url = urljoin(self.__base_url, path)
            resp = await sync_to_async(self.__requests_client.post)(
                url, json=js
            )
            if resp.status_code == 200:
                ret = resp.json()
                return ret
            else:
                msg = resp.text
                raise BrowserOperationError(msg, error_code=resp.status_code)


class BaseSocksCommander:

    def __init__(self, host: str, port: int):
        if not stem.util.connection.is_valid_ipv4_address(host):
            self.host = socket.gethostbyname(host)
        else:
            self.host = host
        if port == 9050:
            self.port = port + 1
        else:
            self.port = port

    def rebuild_nym(self):
        pass


class TorCommander(BaseSocksCommander):

    def __init__(self, host: str, port: int):
        super().__init__(host, port)

    def rebuild_nym(self):
        with stem.control.Controller.from_port(address=self.host, port=self.port) as controller:
            controller.authenticate()
            controller.signal(stem.Signal.NEWNYM)
            sleep(5)
