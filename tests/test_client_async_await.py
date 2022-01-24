import asyncio
import sys
import unittest

from nats.aio.client import Client as NATS
from nats.aio.errors import ErrTimeout
from nats.errors import SlowConsumerError, TimeoutError
from tests.utils import SingleServerTestCase, async_test


class ClientAsyncAwaitTest(SingleServerTestCase):

    @async_test
    async def test_async_await_subscribe_async(self):
        nc = NATS()
        msgs = []

        async def subscription_handler(msg):
            if msg.subject == "tests.1":
                await asyncio.sleep(0.5)
            if msg.subject == "tests.3":
                await asyncio.sleep(0.2)
            msgs.append(msg)

        await nc.connect()
        sub = await nc.subscribe("tests.>", cb=subscription_handler)
        self.assertIsNotNone(sub)

        for i in range(0, 5):
            await nc.publish(f"tests.{i}", b'bar')

        # Wait a bit for messages to be received.
        await asyncio.sleep(1)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        await nc.close()

    @async_test
    async def test_async_await_messages_delivery_order(self):
        nc = NATS()
        msgs = []
        errors = []

        async def error_handler(e):
            errors.append(e)

        await nc.connect(error_cb=error_handler)

        async def handler_foo(msg):
            msgs.append(msg)

            # Should not block other subscriptions from receiving messages.
            await asyncio.sleep(0.2)
            if msg.reply != "":
                await nc.publish(msg.reply, msg.data * 2)

        await nc.subscribe("foo", cb=handler_foo)

        async def handler_bar(msg):
            msgs.append(msg)
            if msg.reply != "":
                await nc.publish(msg.reply, b'')

        await nc.subscribe("bar", cb=handler_bar)

        await nc.publish("foo", b'1')
        await nc.publish("foo", b'2')
        await nc.publish("foo", b'3')

        # Will be processed before the others since no head of line
        # blocking among the subscriptions.
        await nc.publish("bar", b'4')

        response = await nc.request("foo", b'hello1', timeout=1)
        self.assertEqual(response.data, b'hello1hello1')

        with self.assertRaises(TimeoutError):
            await nc.request("foo", b'hello2', timeout=0.1)

        await nc.publish("bar", b'5')
        response = await nc.request("foo", b'hello2', timeout=1)
        self.assertEqual(response.data, b'hello2hello2')

        self.assertEqual(msgs[0].data, b'1')
        self.assertEqual(msgs[1].data, b'4')
        self.assertEqual(msgs[2].data, b'2')
        self.assertEqual(msgs[3].data, b'3')
        self.assertEqual(msgs[4].data, b'hello1')
        self.assertEqual(msgs[5].data, b'hello2')
        self.assertEqual(len(errors), 0)
        await nc.close()

    @async_test
    async def test_subscription_slow_consumer_pending_msg_limit(self):
        nc = NATS()
        msgs = []
        errors = []

        async def error_handler(e):
            if type(e) is SlowConsumerError:
                errors.append(e)

        await nc.connect(error_cb=error_handler)

        async def handler_foo(msg):
            await asyncio.sleep(0.2)

            msgs.append(msg)
            if msg.reply != "":
                await nc.publish(msg.reply, msg.data * 2)

        await nc.subscribe("foo", cb=handler_foo, pending_msgs_limit=5)

        async def handler_bar(msg):
            msgs.append(msg)
            if msg.reply != "":
                await nc.publish(msg.reply, msg.data * 3)

        await nc.subscribe("bar", cb=handler_bar)

        for i in range(10):
            await nc.publish("foo", f'{i}'.encode())

        # Will be processed before the others since no head of line
        # blocking among the subscriptions.
        await nc.publish("bar", b'14')
        response = await nc.request("bar", b'hi1', 2)
        self.assertEqual(response.data, b'hi1hi1hi1')

        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[0].data, b'14')
        self.assertEqual(msgs[1].data, b'hi1')

        # Consumed messages but the rest were slow consumers.
        self.assertTrue(4 <= len(errors) <= 5)
        for e in errors:
            self.assertEqual(type(e), SlowConsumerError)
            self.assertIn("foo", str(e))
            self.assertNotIn("bar", str(e))
        self.assertEqual(errors[0].sid, 1)
        await nc.close()

    @async_test
    async def test_subscription_slow_consumer_pending_bytes_limit(self):
        nc = NATS()
        msgs = []
        errors = []

        async def error_handler(e):
            if type(e) is SlowConsumerError:
                errors.append(e)

        await nc.connect(error_cb=error_handler)

        async def handler_foo(msg):
            await asyncio.sleep(0.2)

            msgs.append(msg)
            if msg.reply != "":
                await nc.publish(msg.reply, msg.data * 2)

        await nc.subscribe("foo", cb=handler_foo, pending_bytes_limit=10)

        async def handler_bar(msg):
            msgs.append(msg)
            if msg.reply != "":
                await nc.publish(msg.reply, msg.data * 3)

        await nc.subscribe("bar", cb=handler_bar)

        for i in range(10):
            await nc.publish("foo", f"AAA{i}".encode())

        # Will be processed before the others since no head of line
        # blocking among the subscriptions.
        await nc.publish("bar", b'14')

        response = await nc.request("bar", b'hi1', 2)
        self.assertEqual(response.data, b'hi1hi1hi1')
        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[0].data, b'14')
        self.assertEqual(msgs[1].data, b'hi1')

        # Consumed a few messages but the rest were slow consumers.
        self.assertTrue(7 <= len(errors) <= 8)
        for e in errors:
            self.assertEqual(type(e), SlowConsumerError)
        self.assertEqual(errors[0].sid, 1)
        self.assertEqual(errors[0].sub._id, 1)
        self.assertEqual(errors[0].reply, "")
        # Try again a few seconds later and it should have recovered
        await asyncio.sleep(3)
        response = await nc.request("foo", b'B', 1)
        self.assertEqual(response.data, b'BB')
        await nc.close()
