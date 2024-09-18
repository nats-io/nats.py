import unittest
import asyncio
import nats.jetstream
import nats

from nats.errors import TimeoutError
from tests.utils import IsolatedJetStreamServerTestCase

class TestPullConsumerFetch(IsolatedJetStreamServerTestCase):
    async def publish_test_msgs(self, js):
        for msg in self.test_msgs:
            await js.publish(self.test_subject, msg.encode())

    async def test_fetch_no_options(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        await self.publish_test_msgs(js)
        msgs = await consumer.fetch(5)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(len(self.test_msgs), len(received_msgs))
        for i, msg in enumerate(received_msgs):
            self.assertEqual(self.test_msgs[i], msg.data.decode())

        self.assertIsNone(msgs.error())

    async def test_delete_consumer_during_fetch(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        await self.publish_test_msgs(js)
        msgs = await consumer.fetch(10)
        await asyncio.sleep(0.1)
        await stream.delete_consumer(consumer.name)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(len(self.test_msgs), len(received_msgs))
        for i, msg in enumerate(received_msgs):
            self.assertEqual(self.test_msgs[i], msg.data.decode())

        self.assertIsInstance(msgs.error(), ConsumerDeletedError)

    async def test_fetch_single_messages_one_by_one(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        received_msgs = []

        async def fetch_messages():
            while len(received_msgs) < len(self.test_msgs):
                msgs = await consumer.fetch(1)
                async for msg in msgs.messages():
                    if msg:
                        received_msgs.append(msg)
                if msgs.error():
                    return

        task = asyncio.create_task(fetch_messages())
        await asyncio.sleep(0.01)
        await self.publish_test_msgs(js)
        await task

        self.assertEqual(len(self.test_msgs), len(received_msgs))
        for i, msg in enumerate(received_msgs):
            self.assertEqual(self.test_msgs[i], msg.data.decode())

    async def test_fetch_no_wait_no_messages(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        msgs = await consumer.fetch_no_wait(5)
        await asyncio.sleep(0.1)
        await self.publish_test_msgs(js)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(0, len(received_msgs))

    async def test_fetch_no_wait_some_messages_available(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        await self.publish_test_msgs(js)
        await asyncio.sleep(0.05)
        msgs = await consumer.fetch_no_wait(10)
        await asyncio.sleep(0.1)
        await self.publish_test_msgs(js)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(len(self.test_msgs), len(received_msgs))

    async def test_fetch_with_timeout(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        msgs = await consumer.fetch(5, max_wait=0.05)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(0, len(received_msgs))

    async def test_fetch_with_invalid_timeout(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        with self.assertRaises(ValueError):
            await consumer.fetch(5, max_wait=-0.05)

    async def test_fetch_with_missing_heartbeat(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        await self.publish_test_msgs(js)
        msgs = await consumer.fetch(5, heartbeat=0.05)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(len(self.test_msgs), len(received_msgs))
        self.assertIsNone(msgs.error())

        msgs = await consumer.fetch(5, heartbeat=0.05, max_wait=0.2)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(0, len(received_msgs))
        self.assertIsNone(msgs.error())

        await stream.delete_consumer(consumer.name)
        msgs = await consumer.fetch(5, heartbeat=0.05)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(0, len(received_msgs))
        self.assertIsInstance(msgs.error(), TimeoutError)

    async def test_fetch_with_invalid_heartbeat(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        with self.assertRaises(ValueError):
            await consumer.fetch(5, heartbeat=20)

        with self.assertRaises(ValueError):
            await consumer.fetch(5, heartbeat=2, max_wait=3)

        with self.assertRaises(ValueError):
            await consumer.fetch(5, heartbeat=-2)

class TestPullConsumerFetchBytes(IsolatedJetStreamTest):
    async def setUp(self):
        await super().setUp()
        self.test_subject = "FOO.123"
        self.msg = b"0123456789"

    async def publish_test_msgs(self, js, count):
        for _ in range(count):
            await js.publish(self.test_subject, self.msg)

    async def test_fetch_bytes_exact_count(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT, name="con")

        await self.publish_test_msgs(js, 5)
        msgs = await consumer.fetch_bytes(300)

        received_msgs = []
        async for msg in msgs.messages():
            await msg.ack()
            received_msgs.append(msg)

        self.assertEqual(5, len(received_msgs))
        self.assertIsNone(msgs.error())

    async def test_fetch_bytes_last_msg_does_not_fit(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT, name="con")

        await self.publish_test_msgs(js, 5)
        msgs = await consumer.fetch_bytes(250)

        received_msgs = []
        async for msg in msgs.messages():
            await msg.ack()
            received_msgs.append(msg)

        self.assertEqual(4, len(received_msgs))
        self.assertIsNone(msgs.error())

    async def test_fetch_bytes_single_msg_too_large(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT, name="con")

        await self.publish_test_msgs(js, 5)
        msgs = await consumer.fetch_bytes(30)

        received_msgs = []
        async for msg in msgs.messages():
            await msg.ack()
            received_msgs.append(msg)

        self.assertEqual(0, len(received_msgs))
        self.assertIsNone(msgs.error())

    async def test_fetch_bytes_timeout(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT, name="con")

        await self.publish_test_msgs(js, 5)
        msgs = await consumer.fetch_bytes(1000, max_wait=0.05)

        received_msgs = []
        async for msg in msgs.messages():
            await msg.ack()
            received_msgs.append(msg)

        self.assertEqual(5, len(received_msgs))
        self.assertIsNone(msgs.error())

    async def test_fetch_bytes_missing_heartbeat(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        msgs = await consumer.fetch_bytes(5, heartbeat=0.05, max_wait=0.2)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(0, len(received_msgs))
        self.assertIsNone(msgs.error())

        await stream.delete_consumer(consumer.name)
        msgs = await consumer.fetch_bytes(5, heartbeat=0.05)

        received_msgs = []
        async for msg in msgs.messages():
            received_msgs.append(msg)

        self.assertEqual(0, len(received_msgs))
        self.assertIsInstance(msgs.error(), TimeoutError)

    async def test_fetch_bytes_invalid_heartbeat(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        with self.assertRaises(ValueError):
            await consumer.fetch_bytes(5, heartbeat=20)

        with self.assertRaises(ValueError):
            await consumer.fetch_bytes(5, heartbeat=2, max_wait=3)

        with self.assertRaises(ValueError):
            await consumer.fetch_bytes(5, heartbeat=-2)

class TestPullConsumerMessages(IsolatedJetStreamTest):
    async def setUp(self):
        await super().setUp()
        self.test_subject = "FOO.123"
        self.test_msgs = ["m1", "m2", "m3", "m4", "m5"]

    async def publish_test_msgs(self, js):
        for msg in self.test_msgs:
            await js.publish(self.test_subject, msg.encode())

    async def test_messages_no_options(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        await self.publish_test_msgs(js)
        msgs = []
        async with consumer.messages() as iterator:
            async for msg in iterator:
                msgs.append(msg)
                if len(msgs) == len(self.test_msgs):
                    break

        self.assertEqual(len(self.test_msgs), len(msgs))
        for i, msg in enumerate(msgs):
            self.assertEqual(self.test_msgs[i], msg.data.decode())

    async def test_messages_delete_consumer_during_iteration(self):
        js = await jetstream.JetStream.connect(self.nc)
        stream = await js.add_stream(name="foo", subjects=["FOO.*"])
        consumer = await stream.create_consumer(jetstream.PullConsumer, ack_policy=jetstream.AckPolicy.EXPLICIT)

        await self.publish_test_msgs(js)
        msgs = []
        async with consumer.messages() as iterator:
            async for msg in iterator:
                msgs.append(msg)
                if len(msgs) == len(self.test_msgs):
                    break

        await stream.delete_consumer(consumer.name)

        with self.assertRaises(ConsumerDeletedError):
            async with consumer.messages() as iterator:
                async for _ in iterator:
                    pass
