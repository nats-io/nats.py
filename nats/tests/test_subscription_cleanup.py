import asyncio
import unittest
from unittest import mock

from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.errors import BadSubscriptionError
from nats.js.client import JetStreamContext


class SubscriptionCleanupTest(unittest.IsolatedAsyncioTestCase):
    async def test_drained_push_subscription_rejects_unsubscribe(self):
        nc = mock.Mock(is_closed=False, is_draining=False, is_reconnecting=False)
        nc._send_unsubscribe = mock.AsyncMock()
        nc.flush = mock.AsyncMock()

        async def callback(msg):
            pass

        sub = Subscription(nc, id=1, cb=callback)
        sub._jsi = mock.Mock(_hbtask=None, _fctask=None)
        sub._start(None)
        push = JetStreamContext.PushSubscription(mock.Mock(), sub, "stream", "consumer")
        try:
            await push.drain()
            assert sub._closed
            with self.assertRaises(BadSubscriptionError):
                await push.unsubscribe()
        finally:
            sub._stop_processing()
            await asyncio.gather(sub._wait_for_msgs_task, return_exceptions=True)

    async def test_unsubscribe_stops_callback_that_suppresses_cancellation(self):
        nc = mock.Mock(is_closed=False, is_draining=False, is_reconnecting=False)
        nc._send_unsubscribe = mock.AsyncMock()
        entered = asyncio.Event()
        calls = []

        async def callback(msg):
            calls.append(msg)
            entered.set()
            if len(calls) > 1:
                return
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                # Older asyncio.wait_for implementations can consume cancellation
                # when the awaited future has just completed.
                pass

        sub = Subscription(nc, id=1, cb=callback)
        first = Msg(nc, data=b"first")
        remaining = Msg(nc, data=b"remaining")
        sub._pending_size = len(first.data) + len(remaining.data)
        sub._pending_queue.put_nowait(first)
        sub._pending_queue.put_nowait(remaining)
        sub._start(None)
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            await sub.unsubscribe()
            await asyncio.wait_for(sub._wait_for_msgs_task, timeout=1)
            assert sub._wait_for_msgs_task.done()
            assert calls == [first]
            assert sub.pending_msgs == 1
            nc._remove_sub.assert_called_once_with(1)
        finally:
            sub._stop_processing()
            await asyncio.gather(sub._wait_for_msgs_task, return_exceptions=True)
