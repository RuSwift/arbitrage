"""In-memory throttler for WS/API rate limiting (no Redis)."""

from datetime import datetime, timedelta
from typing import Any


class Throttler:
    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        self._storage: dict[str, datetime] = {}

    def may_pass(self, name: str, tag: str = "") -> bool:
        key = self._key(name, tag)
        ts = self._storage.get(key)
        now = datetime.utcnow()
        if ts is not None:
            delta_sec = (now - ts).total_seconds()
        else:
            delta_sec = float("inf")
        if delta_sec >= self.timeout:
            self._storage[key] = now
            return True
        return False

    def soon_timeout(self, name: str, tag: str = "") -> float:
        """Seconds until the next call is allowed."""
        key = self._key(name, tag)
        ts = self._storage.get(key)
        if ts is not None:
            till = ts + timedelta(seconds=self.timeout)
            return max(0.0, (till - datetime.utcnow()).total_seconds())
        return 0.0

    @staticmethod
    def _key(name: str, tag: str) -> str:
        return name + "#" + tag
