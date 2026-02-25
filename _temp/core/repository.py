import asyncio
import json
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Optional, Dict, Tuple, AsyncIterator, Union, List, Callable, Awaitable

import aioredis
from django.conf import settings

from core.redis_lock import RedisLock
from core.crud.intf.user import CRUDUserInterface
from core.crud.intf.p2p import CRUDAdvertisers, CRUDAdverts
from core.crud.intf.stats import CRUDStatsInterface


class DistributedLock:

    REDIS_NAMESPACE = 'locks'

    def __init__(self, conn_pool: aioredis.ConnectionPool, key: str):
        self.__conn = aioredis.Redis(connection_pool=conn_pool)
        self.__key = f"{self.REDIS_NAMESPACE}:{key}"
        self.__lock: Optional[RedisLock] = None

    async def acquire(self, timeout: int = 30, wait_timeout: Optional[int] = 30) -> bool:
        """Attempt to acquire the lock

        :param timeout: Number of seconds until the lock should timeout. It can be extended via extend
        :param wait_timeout: How long to wait before aborting the lock request
        :returns: bool, true if acquired false otherwise.
        """
        self.__lock = RedisLock(self.__conn, self.__key)
        success = await self.__lock.acquire(timeout, wait_timeout)
        return success

    async def release(self) -> bool:
        """
        Release the lock, this will only release if this instance of the lock
        is the one holding the lock.
        :returns: bool of the success
        """
        if self.__lock:
            success = await self.__lock.release()
            return success
        else:
            return False

    async def extend(self, added_time: int) -> bool:
        """
        Attempt to extend the lock by the amount of time, this will only extend
        if this instance owns the lock. Note that this is _incremental_, meaning
        that adding time will be on-top of however much time is already available.

        :param added_time: Number of seconds to add to the lock expiration
        :returns: bool of the success
        """
        if self.__lock:
            success = await self.__lock.extend(added_time)
            return success
        else:
            return False

    async def renew(self, timeout: Optional[int] = 30) -> bool:
        """
        Renew the lock, setting expiration to now + timeout (or self.timeout if not provided).
        This will only
        succeed if the instance is the lock owner.

        :returns: True if the release was successful, 0 otherwise.
        """
        if self.__lock:
            success = await self.__lock.renew(timeout)
            return success
        else:
            return False

    @asynccontextmanager
    async def barrier(self, infinite: bool = False, ignore_exceptions: bool = True, timeout: int = 5):
        try:
            # wait indefinitely to acquire a lock
            success = await self.acquire(wait_timeout=None)
            if success:
                # Renew lock in foreground if infinite process
                if infinite:

                    async def __renew_routine():
                        while True:
                            print(f'barrier #1 timeout: {timeout}')
                            await asyncio.sleep(timeout / 3)
                            print('barrier #2')
                            if not await self.renew(timeout):
                                print('barrier #3')
                                return

                    renew_foreground = asyncio.create_task(__renew_routine())
                else:
                    renew_foreground = None
                try:
                    # Switch to working context
                    yield renew_foreground
                finally:
                    renew_foreground.cancel()
        except Exception as e:
            if ignore_exceptions:
                logging.exception('Error!')
            else:
                raise e


class Repository(
    CRUDUserInterface, CRUDAdvertisers, CRUDAdverts, CRUDStatsInterface
):
    """Упрощает работу с слоем хранилища (NoSql, Sql, etc.)
    """

    RECONNECT_TIMEOUT_SEC = 1.0
    NtfCallbackType = Callable[[Dict], Awaitable]

    def __init__(
        self, dsn: str = None, conn_pool: aioredis.ConnectionPool = None
    ):
        if dsn and conn_pool:
            raise AttributeError(
                f'Both assigned dsn and conn_pool not supported!'
            )
        # !!!!! Запрет на исп-ие пула пока не решилась проблема
        # !!!!! RuntimeError: There is no current event loop in thread 'ThreadPoolExecutor-xxx'.   # noqa
        conn_pool = None
        # !!!!!!!!!
        if not dsn and not conn_pool:
            dsn = settings.REDIS_DSN
        if conn_pool:
            self.__conn_pool = conn_pool
        else:
            self.__conn_pool = aioredis.ConnectionPool.from_url(dsn)
        self.__cached_redis: Optional[aioredis.Redis] = None
        self.__subscriptions: Dict[str, asyncio.Task] = {}

    async def set(self, key: Union[str, List[str]], value: Union[Dict, List[Dict]], ttl: int = None):
        async with self.__get_connection() as redis:
            if isinstance(key, str):
                await redis.set(key, json.dumps(value), ex=ttl)
            else:
                if not isinstance(value, List):
                    raise RuntimeError(f'Unexpected value type!')
                if len(key) != len(value):
                    raise RuntimeError(f'Keys and Values arrays must to have the same lengths!')
                mapping = {k: json.dumps(v) for k, v in zip(key, value)}
                await redis.mset(mapping)
                if ttl:
                    logging.warning(f'Too expensive operation to set TTL')
                    for k in key:
                        await redis.expire(k, ttl)

    async def get(self, key: Union[str, List[str]]) -> Optional[Dict]:
        async with self.__get_connection() as redis:
            if isinstance(key, str):
                raw = await redis.get(key)
                if raw is None:
                    return None
                else:
                    return json.loads(raw)
            else:
                raws = await redis.mget(keys=key)
                result = {k: json.loads(v) if v else v for k, v in zip(key, raws)}
                return result

    async def delete(self, key: Union[str, List[str]]):
        async with self.__get_connection() as redis:
            if isinstance(key, str):
                await redis.delete(key)
            else:
                await redis.delete(*key)

    async def publish(self, key: str, value: Dict, ttl: int = None):
        await self.set(key, value, ttl)
        cnt = await self.notify(key, value)
        logging.debug(f'Notify to key: {key} subscr count: {cnt}')

    async def notify(self, key: str, value: Dict) -> int:
        async with self.__get_connection() as redis:
            recipient_count = await redis.publish(key, json.dumps(value))
            return recipient_count

    async def subscribe(
        self, key: Union[str, List[str]],
        on: asyncio.Event = None, cb: NtfCallbackType = None
    ) -> str:
        async def __listener__(_key: Union[str, List[str]], _on: asyncio.Event, _cb: Repository.NtfCallbackType):
            while True:
                try:
                    # Для подписки работаем с отдельным сокетным соединением
                    # во избежание сайдэффектов
                    async with self.__get_connection(new_instance=True) as redis:
                        sub = redis.pubsub()
                        if isinstance(_key, str) and _key.endswith('*'):
                            await sub.psubscribe(_key)
                        else:
                            ch_names = [_key] if isinstance(key, str) else _key
                            await sub.subscribe(*ch_names)
                        try:
                            async for event in sub.listen():
                                if event['type'] in {'message', 'pmessage'}:
                                    try:
                                        msg = json.loads(event['data'])
                                        if _on:
                                            _on.set()
                                        if _cb:
                                            asyncio.ensure_future(_cb(msg))
                                    except Exception as e:
                                        if isinstance(e, asyncio.CancelledError):
                                            raise e
                                        else:
                                            # ignore all subsequent errors
                                            pass
                        finally:
                            await sub.close()
                            await redis.close()
                except Exception as e:
                    if isinstance(e, aioredis.exceptions.ConnectionError):
                        logging.warning(f'Redis listener #1: reconnect to redis...')
                        await asyncio.sleep(self.RECONNECT_TIMEOUT_SEC)
                    else:
                        raise e

        sub_id = uuid.uuid4().hex
        self.__subscriptions[sub_id] = asyncio.create_task(__listener__(key, on, cb))
        return sub_id

    def unsubscribe(self, token: str):
        tsk = self.__subscriptions.pop(token, None)
        if isinstance(tsk, asyncio.Task):
            tsk.cancel()

    async def subscribe_once(self, key: str, on: asyncio.Future) -> str:

        async def __listener_once__(_key: Union[str, List[str]], _on: asyncio.Future):
            while True:
                try:
                    # Для подписки работаем с отдельным сокетным соединением
                    # во избежание сайдэффектов
                    async with self.__get_connection(new_instance=True) as redis:
                        sub = redis.pubsub()
                        if isinstance(_key, str) and _key.endswith('*'):
                            await sub.psubscribe(_key)
                        else:
                            ch_names = [_key] if isinstance(key, str) else _key
                            await sub.subscribe(*ch_names)
                        try:
                            async for event in sub.listen():
                                if event['type'] == 'message':
                                    msg = json.loads(event['data'])
                                    if not _on.cancelled():
                                        _on.set_result(msg)
                                    return
                        finally:
                            await sub.close()
                            await redis.close()
                except Exception as e:
                    if isinstance(e, aioredis.exceptions.ConnectionError):
                        logging.warning(f'Redis listener #2: reconnect to redis...')
                        await asyncio.sleep(self.RECONNECT_TIMEOUT_SEC)
                    else:
                        raise e

        sub_id = uuid.uuid4().hex
        self.__subscriptions[sub_id] = asyncio.create_task(__listener_once__(key, on))
        return sub_id

    async def cas(
            self, key: str, value: Dict, cas_attr: str = None, cas_token: str = None
    ) -> Tuple[bool, str]:
        """Compare And Swap via Redis lua scripting

        :param key: redis key to update
        :param value: redis value
        :param cas_attr: record.cas_attr to compare with token specified in cas_token
        :param cas_token: value to compare with record.cas_attr actual value
        :return:
        """
        script = """
            local key = KEYS[1]
            local value = KEYS[2]
            local cas_attr = ARGV[1]
            local cas_token = ARGV[2]
            
            local ok = true
            local err_msg = 'OK'
            local s = redis.call('GET', key)
            
            if s then
                local js = cjson.decode(s)
                local cas_actual_token_value = js[cas_attr]
                if cas_actual_token_value then
                    if cas_token then
                        ok = cas_actual_token_value == cas_token
                        if not ok then
                            err_msg = 'cas_token value differs from actual record value'
                        end
                    end
                else
                    ok = false
                    err_msg = 'cas_attr missing in actual redis record for specified key'
                end
            end
            if ok then
                redis.call('SET', key, value)
                err_msg = 'value set forced'
            end
            return {ok, err_msg}
        """
        async with self.__get_connection() as redis:
            res = await redis.eval(script, 2, key, json.dumps(value), cas_attr, cas_token)
            ok, err_msg = res
            ok = True if ok else False
            return ok, err_msg

    def new_lock(self, key: str) -> DistributedLock:
        return DistributedLock(self.__conn_pool, key)

    async def flush_all(self):
        async with self.__get_connection() as redis:
            await redis.flushall()

    async def load_batched(self, values: Dict, ttl: int = None):
        for key, value in values.items():
            if not isinstance(value, Dict):
                raise RuntimeError(f'Value with key: {key} has impossible type!')
            await self.set(key, value, ttl)

    @asynccontextmanager
    async def __get_connection(self, new_instance: bool = False) -> AsyncIterator[aioredis.Redis]:
        if new_instance:
            redis = aioredis.Redis(connection_pool=self.__conn_pool)
        else:
            if not self.__cached_redis:
                self.__cached_redis = aioredis.Redis(connection_pool=self.__conn_pool)
            redis = self.__cached_redis
        yield redis
