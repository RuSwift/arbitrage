"""Throttler tests (Redis-backed). Require Redis; connection from .env."""
import time

import pytest

from app.cex.throttler import Throttler


class TestThrottler:
    """Sync tests for Throttler with Redis (fixture from conftest)."""

    def test_pass(self, throttler: Throttler) -> None:
        names = ["name1", "name2", "name3"]
        for name in names:
            assert throttler.may_pass(name) is True

        time.sleep(throttler.timeout * 1.1)

        for name in names:
            assert throttler.may_pass(name) is True

    def test_deny(self, throttler: Throttler) -> None:
        names = ["name1", "name2", "name3"]
        for name in names:
            assert throttler.may_pass(name) is True

        for name in names:
            assert throttler.may_pass(name) is False

    def test_soon_timeout(self, throttler: Throttler) -> None:
        name = "name"
        assert throttler.soon_timeout(name) == 0.0

        assert throttler.may_pass(name) is True
        value = throttler.soon_timeout(name)
        assert throttler.timeout * 0.99 < value < throttler.timeout * 1.01

    def test_tag_isolation(self, throttler: Throttler) -> None:
        """Different tags are throttled independently."""
        assert throttler.may_pass("sym", tag="book") is True
        assert throttler.may_pass("sym", tag="depth") is True
        assert throttler.may_pass("sym", tag="book") is False
        assert throttler.may_pass("sym", tag="depth") is False
