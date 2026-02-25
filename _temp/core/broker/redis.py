import json
from typing import AsyncGenerator

import aioredis
from django.conf import settings

from .base import AbstractMessageBroker


class RedisBroker(AbstractMessageBroker):

    def __init__(self, namespace: str = ''):
        self._namespace = namespace
        self._redis = aioredis.Redis(connection_pool=settings.REDIS_CONN_POOL)

    async def post(self, topic: str, value: AbstractMessageBroker.DATA_TYPE):
        await self.send(topic, value)

    async def send(
        self, topic: str, value: AbstractMessageBroker.DATA_TYPE
    ) -> int:
        recipient_count = await self._redis.publish(
            self._build_key(topic), json.dumps(value)
        )
        return recipient_count

    async def receive(
        self, topic: str, timeout: float = None
    ) -> AbstractMessageBroker.DATA_TYPE:
        sub = self._redis.pubsub()
        ch_name = self._build_key(topic)
        await sub.subscribe(ch_name)
        raw = await sub.get_message(
            ignore_subscribe_messages=True,
            timeout=timeout
        )
        try:
            if raw:
                return json.loads(raw)
            else:
                raise RuntimeError('Empty data in channel')
        finally:
            await sub.unsubscribe()

    async def listen(
        self, topic: str
    ) -> AsyncGenerator[AbstractMessageBroker.DATA_TYPE, None]:
        sub = self._redis.pubsub()
        ch_name = self._build_key(topic)
        await sub.subscribe(ch_name)
        try:
            async for event in sub.listen():
                if event['type'] == 'message':
                    msg = json.loads(event['data'])
                    yield msg
        finally:
            await sub.unsubscribe()

    def _build_key(self, topic: str) -> str:
        return f'{self._namespace}/{topic}'
