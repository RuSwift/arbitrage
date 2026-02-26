import asyncio
import uuid
from time import sleep

import pytest

from connectors.scheduling import *


class TestWsListener(WebSocketAssist.Listener):

    def __init__(self):
        self.events = list()

    def on(self, data: Optional[Any], closed: bool):
        self.events.append([data, closed])


class TestCoroutinesThreadScheduler:

    @pytest.fixture
    def scheduler(self) -> CoroutinesThreadScheduler:
        return CoroutinesThreadScheduler()

    def test_sane(self, scheduler: CoroutinesThreadScheduler):
        assert scheduler.loop is None
        assert scheduler.started is False
        scheduler.start()
        sleep(0.1)
        assert scheduler.started is True
        scheduler.stop()
        sleep(0.1)
        assert scheduler.started is False
        assert scheduler.loop is None

    def test_run_coroutines(self, scheduler: CoroutinesThreadScheduler):
        value = None

        async def co():
            nonlocal value
            value = 1
            return value

        scheduler.start()
        try:
            fut = scheduler.run_detached(co())
            sleep(0.1)
            assert value == 1
            assert fut.done() and fut.result() == 1
        finally:
            scheduler.stop()

    def test_timeouts(self, scheduler: CoroutinesThreadScheduler):

        async def co(delay_: float):
            await asyncio.sleep(delay_)
            return 'Result'

        scheduler.start()
        try:
            res = scheduler.run_and_wait_result(co=co(delay_=1.0), timeout=3.0)
            assert res == 'Result'

            with pytest.raises(Exception) as e:
                scheduler.run_and_wait_result(co=co(delay_=3.0), timeout=1.0)
            assert 'Timeout' in str(e)
        finally:
            scheduler.stop()


class TestWebsocketAssist:

    @pytest.fixture
    def okx_assist(self) -> WebSocketAssist:
        return WebSocketAssist(url='wss://ws.okx.com:8443/ws/v5/public')

    @pytest.fixture
    def echo_assist(self) -> WebSocketAssist:
        return WebSocketAssist(url='wss://ws.postman-echo.com/raw')

    def test_connect_disconnect(self, okx_assist: WebSocketAssist):
        assert okx_assist.is_connected is False
        ok = okx_assist.connect()
        assert ok is True
        assert okx_assist.is_connected is True
        okx_assist.disconnect()
        assert okx_assist.is_connected is False

    def test_send_and_rcv(self, echo_assist: WebSocketAssist):
        ok = echo_assist.connect()
        assert ok
        test_text = 'hello-' + uuid.uuid4().hex
        echo_assist.send(payload={'message': test_text})
        resp = echo_assist.read(timeout=5.0)
        assert resp
        assert resp['message'] == test_text

    def test_listeners(self, okx_assist: WebSocketAssist):
        assert okx_assist.is_connected is False
        ok = okx_assist.connect()
        assert ok is True
        try:
            listener_under_test = TestWsListener()
            okx_assist.subscribe(listener_under_test)
            cmd = {"op": "subscribe", "args": [{"channel": "tickers", "instId": "XRP-USDT"}]}
            okx_assist.send(cmd)
            sleep(3)
            okx_assist.unsubscribe()
        finally:
            okx_assist.disconnect()
        assert len(listener_under_test.events) > 1
        assert all([e[1] is False for e in listener_under_test.events[:-1]])
        last_event = listener_under_test.events[-1]
        assert last_event[0] is None
        assert last_event[1] is True
