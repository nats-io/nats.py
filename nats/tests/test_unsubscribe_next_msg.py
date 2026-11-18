import asyncio

from nats import errors
from nats.aio.client import Client as NATS

from tests.utils import SingleServerTestCase, async_test


class UnsubscribeNextMsgTest(SingleServerTestCase):
    async def _wait_for_next_msg_calls(self, sub, count):
        async def wait_for_calls():
            while len(sub._pending_next_msgs_calls) != count:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_calls(), 1)

    @async_test
    async def test_unsubscribe_wakes_all_pending_next_msg_calls(self):
        nc = NATS()
        await nc.connect()
        try:
            for timeout in (30, None):
                with self.subTest(timeout=timeout):
                    sub = await nc.subscribe("foo")
                    tasks = [asyncio.create_task(sub.next_msg(timeout=timeout)) for _ in range(3)]
                    try:
                        await self._wait_for_next_msg_calls(sub, len(tasks))
                        await sub.unsubscribe()
                        done, pending = await asyncio.wait(tasks, timeout=1)
                        self.assertFalse(pending, "unsubscribe left next_msg calls waiting")
                        self.assertEqual(len(done), len(tasks))
                        for task in tasks:
                            with self.assertRaises(errors.BadSubscriptionError):
                                await task
                        self.assertEqual(sub._pending_next_msgs_calls, {})
                        self.assertNotIn(sub._id, nc._subs)
                    finally:
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await nc.close()

    @async_test
    async def test_unsubscribe_preserves_buffered_messages_and_other_subscriptions(self):
        nc = NATS()
        await nc.connect()
        try:
            sub = await nc.subscribe("foo")
            other = await nc.subscribe("bar")
            await nc.publish("foo", b"buffered")

            async def wait_for_buffered_message():
                while sub.pending_msgs != 1:
                    await asyncio.sleep(0)

            await asyncio.wait_for(wait_for_buffered_message(), 1)
            self.assertEqual(sub.pending_msgs, 1)
            await sub.unsubscribe()
            self.assertEqual((await sub.next_msg()).data, b"buffered")
            self.assertEqual(sub.pending_msgs, 0)
            self.assertEqual(sub.pending_bytes, 0)
            await nc.publish("bar", b"live")
            self.assertEqual((await other.next_msg()).data, b"live")
        finally:
            await nc.close()

    @async_test
    async def test_deferred_unsubscribe_keeps_next_msg_waiting_for_delivery(self):
        nc = NATS()
        await nc.connect()
        try:
            sub = await nc.subscribe("foo")
            task = asyncio.create_task(sub.next_msg(timeout=None))
            try:
                await self._wait_for_next_msg_calls(sub, 1)
                await sub.unsubscribe(limit=1)
                self.assertFalse(sub._closed)
                self.assertFalse(task.done())
                await nc.publish("foo", b"last")
                self.assertEqual((await asyncio.wait_for(task, 1)).data, b"last")
                self.assertEqual(sub._pending_next_msgs_calls, {})
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        finally:
            await nc.close()

    @async_test
    async def test_close_preserves_connection_closed_error(self):
        nc = NATS()
        await nc.connect()
        try:
            sub = await nc.subscribe("foo")
            task = asyncio.create_task(sub.next_msg(timeout=None))
            try:
                await self._wait_for_next_msg_calls(sub, 1)
                await nc.close()
                with self.assertRaises(errors.ConnectionClosedError):
                    await asyncio.wait_for(task, 1)
                self.assertEqual(sub._pending_next_msgs_calls, {})
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        finally:
            await nc.close()

    @async_test
    async def test_caller_cancellation_preserves_cancelled_error(self):
        nc = NATS()
        await nc.connect()
        try:
            sub = await nc.subscribe("foo")
            task = asyncio.create_task(sub.next_msg(timeout=None))
            try:
                await self._wait_for_next_msg_calls(sub, 1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertFalse(sub._closed)
                self.assertEqual(sub._pending_next_msgs_calls, {})
                await nc.publish("foo", b"still live")
                self.assertEqual((await sub.next_msg()).data, b"still live")
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        finally:
            await nc.close()

    @async_test
    async def test_live_subscription_preserves_timeout_error(self):
        nc = NATS()
        await nc.connect()
        try:
            sub = await nc.subscribe("foo")
            with self.assertRaises(errors.TimeoutError):
                await sub.next_msg(timeout=0.01)
            self.assertFalse(sub._closed)
            self.assertEqual(sub._pending_next_msgs_calls, {})
        finally:
            await nc.close()
