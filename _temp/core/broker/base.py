from typing import Dict, List, Union
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator


class AbstractMessageBroker(ABC):

    DATA_TYPE = Union[Dict, List]

    @abstractmethod
    async def post(self, topic: str, value: DATA_TYPE):
        pass

    @abstractmethod
    async def send(self, topic: str, value: DATA_TYPE) -> int:
        pass

    @abstractmethod
    async def receive(self, topic: str, timeout: float = None) -> DATA_TYPE:
        pass

    @abstractmethod
    async def listen(self, topic: str) -> AsyncGenerator[DATA_TYPE]:
        pass
