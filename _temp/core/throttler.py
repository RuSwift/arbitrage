from typing import Dict
from datetime import datetime, timedelta


class Throttler:

    def __init__(self, timeout: float):
        self.timeout = timeout
        self.__storage: Dict[str, datetime] = {}

    def may_pass(self, name: str, tag: str = '') -> bool:
        name_with_tag = self.__name_with_tag(name, tag)
        ts = self.__storage.get(name_with_tag)
        now = datetime.utcnow()
        if ts:
            delta_sec = (now - ts).total_seconds()
        else:
            delta_sec = float('inf')

        if delta_sec > self.timeout:
            self.__storage[name_with_tag] = now
            return True
        else:
            return False

    def soon_timeout(self, name: str, tag: str = '') -> float:
        """Timeout when can call operation"""
        name_with_tag = self.__name_with_tag(name, tag)
        ts = self.__storage.get(name_with_tag)
        if ts:
            now = datetime.utcnow()
            till = now + timedelta(seconds=self.timeout)
            return (till - now).total_seconds()
        else:
            return 0

    @staticmethod
    def __name_with_tag(name: str, tag: str) -> str:
        return name + '#' + tag
