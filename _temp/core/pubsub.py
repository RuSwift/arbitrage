import os
import json
from typing import Generator, Dict, Optional

import aio_pika
import aio_pika.abc
from django.conf import settings

import settings.base


class RMQConn:

    def __init__(self):
        self.__conn: Optional[aio_pika.abc.AbstractConnection] = None
        self.__channel: Optional[aio_pika.abc.AbstractChannel] = None

    async def get_conn(self) -> aio_pika.abc.AbstractConnection:
        if not (self.__conn and self.__conn.connected):
            self._reset()
            self.__conn = await aio_pika.connect(self.__rabbit_dsn())
        return self.__conn

    async def get_channel(self) -> aio_pika.abc.AbstractChannel:
        if not (self.__channel and not self.__channel.is_closed):
            self._reset()
            conn = await self.get_conn()
            self.__channel = await conn.channel()
        return self.__channel

    def _reset(self):
        pass

    @staticmethod
    def __rabbit_dsn() -> str:
        # TODO: Почему то в тестах не подгружается актуальный settings dоango-проекта
        try:
            dsn = settings.RMQ_DSN
        except AttributeError:
            dsn = os.getenv('RMQ_DSN')
        return dsn


class LoadBalancer(RMQConn):
    """Балансировщик нагрузки через Rabbit MQ"""

    def __init__(self, name: str, persistent: bool = False, durable: bool = False):
        """
        :param name: имя очереди
        """
        super().__init__()
        self.__name = name
        self.__persistent = persistent
        self.__durable = durable
        self.__queue: Optional[aio_pika.Queue] = None

    @property
    def name(self) -> str:
        return self.__name

    async def send(self, e: Dict):
        """
        :param e: отправить событие как producer
        """
        await self.__ensure_queue_exists()
        ch = await self.get_channel()
        message = aio_pika.Message(
            body=json.dumps(e).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT if self.__persistent else aio_pika.DeliveryMode.NOT_PERSISTENT,
        )
        await ch.default_exchange.publish(
            message, routing_key=self.__name,
        )

    async def fetch_one(self, timeout: float) -> Optional[Dict]:
        """ Выкачать из очереди событие как RMQ Worker
        :return: объект события
        """
        queue = await self.__ensure_queue_exists()
        msg: Optional[aio_pika.IncomingMessage] = await queue.get(timeout=timeout, fail=False, no_ack=True)
        if msg:
            d = json.loads(msg.body.decode())
            return d
        else:
            return None

    async def fetch_all(self) -> Generator[Dict, None, None]:
        """Выкачиваем все данные из очереди"""

        queue = await self.__ensure_queue_exists()
        async with queue.iterator() as queue_iter:
            message: aio_pika.IncomingMessage
            async for message in queue_iter:
                async with message.process():
                    d = json.loads(message.body.decode())
                    yield d

    def _reset(self):
        self.__queue = None

    async def __ensure_queue_exists(self) -> aio_pika.Queue:
        if self.__queue is None:
            ch = await self.get_channel()
            self.__queue = await ch.declare_queue(name=self.__name, durable=self.__durable)
        return self.__queue
