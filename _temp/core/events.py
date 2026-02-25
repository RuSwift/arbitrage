import asyncio
from typing import Any, List, Union
from contextlib import contextmanager


class Promise:

    def __init__(self, fut: asyncio.Future):
        self.__fut = fut
        self.__triggered = False

    def set_result(self, result: Any):
        with self._process_trigger_flag():
            self.__fut.set_result(result)

    def set_exception(self, exc: BaseException):
        with self._process_trigger_flag():
            self.__fut.set_exception(exc)

    def cancel(self, msg: str = None):
        with self._process_trigger_flag():
            self.__fut.cancel(msg)

    @contextmanager
    def _process_trigger_flag(self):
        if self.__triggered:
            raise RuntimeError('Promise already triggered')
        yield
        self.__triggered = True


class FuturePromise:

    def __init__(self):
        self.__future = asyncio.Future()
        self.__promise = Promise(self.__future)

    @property
    def future(self) -> asyncio.Future:
        return self.__future

    @property
    def promise(self) -> Promise:
        return self.__promise


class ReadonlyStream:

    def __init__(self, q: asyncio.Queue):
        self.__q = q
        self.__cached = list()

    async def read(self, timeout: float = None) -> Any:
        if not self.buffer_empty():
            if self.__cached:
                value = self.__cached.pop()
            else:
                value = self.__q.get_nowait()
            return value
        else:
            if timeout:
                return await asyncio.wait_for(self.__q.get(), timeout=timeout)
            else:
                return await self.__q.get()

    async def wait_for_ready(self, timeout: float = None) -> bool:
        if not self.buffer_empty():
            return True
        try:
            value = await self.read(timeout)
        except asyncio.exceptions.TimeoutError:
            return False
        else:
            print('\n***** cache value *******')
            self.__cached.append(value)
            return True

    def buffer_empty(self) -> bool:
        return not self.__cached and self.__q.empty()


class WriteonlyStream:

    def __init__(self, q: asyncio.Queue):
        self.__q = q

    async def write(self, value: Any):
        await self.__q.put(value)
