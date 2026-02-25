import asyncio
from typing import List, Optional
from copy import copy
from random import shuffle

from settings.base import REDIS_CONN_POOL, cfg as app_cfg
from core.repository import Repository
from core.utils import utc_now_float


class SocksProxyPool:

    LOCK_TIMEOUT_SEC = 15

    def __init__(
        self, sock_addrs: List[str],
        lock_name: str, is_tor: bool, no_lock: bool = False
    ):
        self._repo = Repository(conn_pool=REDIS_CONN_POOL)
        if not sock_addrs:
            raise RuntimeError('Empty sock_addr list')
        self._sock_addrs = copy(sock_addrs)
        shuffle(self._sock_addrs)
        self._lock_name = lock_name
        self._no_lock = no_lock
        self.refresh_task: Optional[asyncio.Task] = None
        self.is_tor = is_tor

    async def lock(self) -> Optional[str]:
        if self._no_lock:
            return self._sock_addrs[0]
        for addr in self._sock_addrs:
            key = self._build_lock_key(addr)
            retrieved = await self._repo.get(key)
            if retrieved and 'expire_at' in retrieved:
                cut_time = utc_now_float()
                if retrieved['expire_at'] < cut_time:
                    await self._repo.delete(key)
        for addr in self._sock_addrs:
            key = self._build_lock_key(addr)
            desc = {
                'locker_name': self._lock_name,
                'expire_at': utc_now_float() + self.LOCK_TIMEOUT_SEC
            }
            ok, _ = await self._repo.cas(
                key, value=desc,
                cas_attr='locker_name', cas_token=self._lock_name
            )
            if ok:
                self.refresh_task = asyncio.create_task(
                    self._refresh_routine(addr)
                )
                return addr
        return None

    async def _refresh_routine(self, addr: str):
        if self._no_lock:
            while True:
                await asyncio.sleep(1000)
        else:
            key = self._build_lock_key(addr)
            while True:
                await asyncio.sleep(self.LOCK_TIMEOUT_SEC/3)
                desc = {
                    'locker_name': self._lock_name,
                    'expire_at': utc_now_float() + self.LOCK_TIMEOUT_SEC
                }
                ok, _ = await self._repo.cas(
                    key, value=desc,
                    cas_attr='locker_name', cas_token=self._lock_name
                )
                if not ok:
                    return

    @classmethod
    def _build_lock_key(cls, addr: str) -> str:
        return f'{app_cfg.distributed_lock.namespaces.socks_pool}/{addr}'
