import asyncio

import pytest

from core.throttler import Throttler


@pytest.mark.asyncio
class TestCoreDefinitions:

    @pytest.fixture()
    def timeout(self) -> float:
        return 1.0

    @pytest.fixture
    def throttler(self, timeout: float) -> Throttler:
        return Throttler(timeout)

    async def test_pass(self, throttler: Throttler):
        names = ['name1', 'name2', 'name3']
        for name in names:
            success = throttler.may_pass(name)
            assert success is True

        await asyncio.sleep(throttler.timeout*1.1)

        for name in names:
            success = throttler.may_pass(name)
            assert success is True

    async def test_deny(self, throttler: Throttler):
        names = ['name1', 'name2', 'name3']
        for name in names:
            success = throttler.may_pass(name)
            assert success is True

        for name in names:
            success = throttler.may_pass(name)
            assert success is False

    async def test_soon_timeout(self, throttler: Throttler, timeout: float):
        name = 'name'
        value = throttler.soon_timeout(name)
        assert value == 0

        ok = throttler.may_pass(name)
        assert ok is True
        value = throttler.soon_timeout(name)
        assert timeout*0.99 < value < timeout*1.01, '~1%'
