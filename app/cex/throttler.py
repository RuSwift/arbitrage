"""Throttler for WS/API rate limiting via Redis."""

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis

logger = logging.getLogger(__name__)

# Lua: allow if key missing or (now - last_ts) >= timeout; then set key = now, pexpire, return 1 else return 0
_MAY_PASS_SCRIPT = """
local key = KEYS[1]
local now_sec = tonumber(ARGV[1])
local timeout_sec = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])
local last = redis.call('GET', key)
if last == false then
  redis.call('SET', key, now_sec)
  redis.call('PEXPIRE', key, ttl_ms)
  return 1
end
local last_sec = tonumber(last)
if (now_sec - last_sec) >= timeout_sec then
  redis.call('SET', key, now_sec)
  redis.call('PEXPIRE', key, ttl_ms)
  return 1
end
return 0
"""


class Throttler:
    """Rate limiter backed by Redis. Requires redis_url."""

    def __init__(
        self,
        timeout: float,
        redis_url: str,
        key_prefix: str = "arbitrage:throttle",
    ) -> None:
        self.timeout = timeout
        self._redis_url = redis_url
        self._key_prefix = key_prefix
        self._client: redis.Redis | None = None
        self._script_sha: str | None = None

    def _get_client(self) -> "redis.Redis":
        if self._client is None:
            import redis
            self._client = redis.from_url(self._redis_url)
        return self._client

    def _redis_key(self, name: str, tag: str) -> str:
        return f"{self._key_prefix}:{self._key(name, tag)}"

    def may_pass(self, name: str, tag: str = "") -> bool:
        try:
            client = self._get_client()
            key = self._redis_key(name, tag)
            now = time.time()
            ttl_ms = int((self.timeout * 2) * 1000)
            if self._script_sha is None:
                self._script_sha = client.script_load(_MAY_PASS_SCRIPT)
            result = client.evalsha(
                self._script_sha, 1, key, str(now), str(self.timeout), str(ttl_ms)
            )
            return bool(result)
        except (ConnectionError, TimeoutError, Exception) as e:
            logger.warning("Throttler Redis error in may_pass: %s", e)
            return False

    def soon_timeout(self, name: str, tag: str = "") -> float:
        """Seconds until the next call is allowed."""
        try:
            client = self._get_client()
            key = self._redis_key(name, tag)
            last = client.get(key)
            if last is None:
                return 0.0
            last_sec = float(last)
            now = time.time()
            delta = now - last_sec
            return max(0.0, self.timeout - delta)
        except (ConnectionError, TimeoutError, Exception) as e:
            logger.warning("Throttler Redis error in soon_timeout: %s", e)
            return 0.0

    @staticmethod
    def _key(name: str, tag: str) -> str:
        return name + "#" + tag
