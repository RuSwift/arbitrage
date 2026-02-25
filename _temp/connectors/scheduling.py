import abc
import asyncio
import threading
from queue import Queue
from concurrent.futures import Future as ConcurrentFuture
from typing import Optional, Coroutine, Any

import aiohttp


class CoroutinesThreadScheduler:

    def __init__(self):
        self.__th = threading.Thread()
        self.__loop: Optional[asyncio.BaseEventLoop] = None
        self.__thread: Optional[threading.Thread] = None
        self.__started = False

    def __del__(self):
        if self.__started:
            self.stop()

    @property
    def started(self) -> bool:
        return self.__started and self.__thread.is_alive()

    @property
    def loop(self) -> Optional[asyncio.AbstractEventLoop]:
        return self.__loop

    def start(self):
        if not self.started:
            self.__loop = asyncio.new_event_loop()
            self.__thread = threading.Thread(
                target=self.__run_forever,
                args=(
                    self.__loop,
                )
            )
            self.__thread.daemon = True
            self.__thread.start()
            self.__started = True

    def run_detached(self, co: Coroutine) -> ConcurrentFuture:
        if self.started:
            fut = asyncio.run_coroutine_threadsafe(coro=co, loop=self.__loop)
            return fut
        else:
            raise RuntimeError('Scheduler is not running')

    def run_and_wait_result(self, co: Coroutine, timeout: Optional[float]) -> Any:
        fut = self.run_detached(co)
        return fut.result(timeout)

    def stop(self):
        if self.started:
            asyncio.run_coroutine_threadsafe(
                coro=self.__terminate_loop(self.__loop),
                loop=self.__loop
            )
            self.__loop = None
            self.__started = False

    @staticmethod
    def __run_forever(loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    @staticmethod
    async def __terminate_loop(loop: asyncio.BaseEventLoop):
        loop.stop()


class WebSocketAssist:

    DEF_TIMEOUT = 3.0

    class Listener:

        @abc.abstractmethod
        def on(self, data: Optional[Any], closed: bool):
            raise NotImplemented

    def __init__(self, url: str):
        self.url = url
        self.__scheduler = CoroutinesThreadScheduler()
        self.__events_queue = Queue()
        self.__session = None
        self.__ws = None
        self.__listener: Optional[ConcurrentFuture] = None

    def connect(self, timeout: float = DEF_TIMEOUT) -> bool:
        self.__scheduler.start()
        ok = self.__scheduler.run_and_wait_result(self.__create_connection(), timeout=timeout)
        return ok

    def disconnect(self):
        if self.is_connected:
            self.__scheduler.run_and_wait_result(self.__close_connection(), timeout=self.DEF_TIMEOUT)
            if self.__listener is not None:
                self.unsubscribe()

    @property
    def is_connected(self) -> bool:
        return (self.__ws is not None) and (not self.__ws.closed)

    def send(self, payload: dict):
        self.__scheduler.run_and_wait_result(self.__send(payload), timeout=self.DEF_TIMEOUT)

    def read(self, timeout: float = None) -> Any:
        if self.is_connected:
            resp = self.__scheduler.run_and_wait_result(self.__read(timeout), timeout=timeout)
            return resp
        else:
            return False, None

    def subscribe(self, listener: Listener):
        if self.__listener is not None:
            raise RuntimeError('Listener already set, unsubscribe first!')
        self.__listener = self.__scheduler.run_detached(self.__listen(listener))

    def unsubscribe(self):
        if self.__listener is not None:
            self.__listener.cancel()
            self.__listener = None

    async def __create_connection(self) -> bool:
        self.__session = aiohttp.ClientSession()
        self.__ws = await self.__session.ws_connect(self.url)
        return True

    async def __close_connection(self):
        await self.__session.close()
        self.__ws = None
        self.__session = None

    async def __send(self, payload: dict):
        await self.__ws.send_json(payload)

    async def __read(self, timeout: float = None) -> Any:
        resp = await self.__ws.receive_json(timeout=timeout)
        return resp

    async def __listen(self, listener: Listener):
        try:
            while not self.__ws.closed:
                data = await self.__ws.receive_json()
                listener.on(data=data, closed=False)
        finally:
            listener.on(data=None, closed=True)
