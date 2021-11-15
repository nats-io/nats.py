import asyncio
import http.client
import json
import ssl
import time
import unittest
import datetime
from unittest import mock
import tempfile
import shutil

import nats
import nats.js.api
from nats.aio.client import Client as NATS
from nats.aio.client import __version__
from nats.errors import *
from nats.aio.errors import *
from nats.js.errors import *
from tests.utils import *


class PublishTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_publish(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()

        with self.assertRaises(NoStreamResponseError):
            await js.publish("foo", b'bar')

        await js.add_stream(name="QUUX", subjects=["quux"])

        ack = await js.publish("quux", b'bar:1', stream="QUUX")
        self.assertEqual(ack.stream, "QUUX")
        self.assertEqual(ack.seq, 1)

        ack = await js.publish("quux", b'bar:2')
        self.assertEqual(ack.stream, "QUUX")
        self.assertEqual(ack.seq, 2)

        with self.assertRaises(BadRequestError) as err:
            await js.publish("quux", b'bar', stream="BAR")
        self.assertEqual(err.exception.err_code, 10060)

        await nc.close()


class PullSubscribeTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_auto_create_consumer(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()
        sinfo = await js.add_stream(name="TEST2", subjects=["a2"])
        sub = await js.pull_subscribe("a2", "auto")
        await js.publish("a2", b'one')

        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        self.assertEqual(msg.data, b'one')

        # Customize consumer config.
        sub = await js.pull_subscribe(
            "a2", "auto2", config=nats.js.api.ConsumerConfig(max_waiting=10)
        )
        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        self.assertEqual(msg.data, b'one')

        info = await js.consumer_info("TEST2", "auto2")
        self.assertEqual(info.config.max_waiting, 10)

        await nc.close()
        await asyncio.sleep(1)

    @async_test
    async def test_fetch_one(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        sinfo = await js.add_stream(name="TEST1", subjects=["foo.1", "bar"])

        # cinfo = await js.add_consumer(
        #     "TEST1", durable_name="dur", ack_policy=api.AckPolicy.explicit
        # )

        ack = await js.publish("foo.1", f'Hello from NATS!'.encode())
        self.assertEqual(ack.stream, "TEST1")
        self.assertEqual(ack.seq, 1)

        # Bind to the consumer that is already present.
        sub = await js.pull_subscribe("foo.1", "dur")
        msgs = await sub.fetch(1)
        for msg in msgs:
            await msg.ack()

        msg = msgs[0]
        self.assertEqual(msg.metadata.sequence.stream, 1)
        self.assertEqual(msg.metadata.sequence.consumer, 1)
        self.assertTrue(datetime.datetime.now() > msg.metadata.timestamp)
        self.assertEqual(msg.metadata.num_pending, 0)
        self.assertEqual(msg.metadata.num_delivered, 1)

        with self.assertRaises(asyncio.TimeoutError):
            await sub.fetch(timeout=1)

        for i in range(0, 10):
            await js.publish(
                "foo.1", f"i:{i}".encode(), headers={'hello': 'world'}
            )

        # nak
        msgs = await sub.fetch()
        msg = msgs[0]

        # FIXME: This is failing with a Request Timeout error on CI
        info = await js.consumer_info("TEST1", "dur", timeout=1)
        self.assertEqual(msg.header, {'hello': 'world'})

        await msg.nak()

        info = await js.consumer_info("TEST1", "dur", timeout=1)
        self.assertEqual(info.stream_name, "TEST1")
        self.assertEqual(info.num_ack_pending, 1)
        self.assertEqual(info.num_redelivered, 0)

        # in_progress
        msgs = await sub.fetch()
        for msg in msgs:
            await msg.in_progress()

        # term
        msgs = await sub.fetch()
        for msg in msgs:
            await msg.term()

        info = await js.consumer_info("TEST1", "dur", timeout=1)
        self.assertEqual(info.num_ack_pending, 1)
        self.assertEqual(info.num_redelivered, 1)

        await nc.close()

    @async_test
    async def test_fetch_max_waiting(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="TEST3", subjects=["max"])

        sub = await js.pull_subscribe(
            "max", "example", config={'max_waiting': 3}
        )
        results = None
        try:
            results = await asyncio.gather(
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                return_exceptions=True,
            )
        except:
            pass

        err = None
        for e in results:
            if isinstance(e, asyncio.TimeoutError):
                continue
            else:
                self.assertIsInstance(e, APIError)
                err = e
                break

        # Should get at least one Request Timeout error.
        self.assertEqual(e.code, 408)
        info = await js.consumer_info("TEST3", "example")
        self.assertEqual(info.num_waiting, 3)
        await nc.close()


class JSMTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_stream_management(self):
        nc = NATS()
        await nc.connect()
        jsm = nc.jsm()

        acc = await jsm.account_info()
        self.assertIsInstance(acc, nats.js.api.AccountInfo)

        # Create stream
        stream = await jsm.add_stream(
            name="hello", subjects=["hello", "world", "hello.>"]
        )
        self.assertIsInstance(stream, nats.js.api.StreamInfo)
        self.assertIsInstance(stream.config, nats.js.api.StreamConfig)
        self.assertEqual(stream.config.name, "hello")
        self.assertIsInstance(stream.state, nats.js.api.StreamState)

        # Get info
        current = await jsm.stream_info("hello")
        stream.did_create = None
        self.assertEqual(stream, current)

        self.assertIsInstance(current, nats.js.api.StreamInfo)
        self.assertIsInstance(current.config, nats.js.api.StreamConfig)
        self.assertEqual(current.config.name, "hello")
        self.assertIsInstance(current.state, nats.js.api.StreamState)

        # Send messages
        producer = nc.jetstream()
        ack = await producer.publish('world', b'Hello world!')
        self.assertEqual(ack.stream, "hello")
        self.assertEqual(ack.seq, 1)

        current = await jsm.stream_info("hello")
        self.assertEqual(current.state.messages, 1)
        self.assertEqual(current.state.bytes, 47)

        # Delete stream
        is_deleted = await jsm.delete_stream("hello")
        self.assertTrue(is_deleted)

        # Not foundError since there is none
        with self.assertRaises(NotFoundError):
            await jsm.stream_info("hello")

        await nc.close()

    @async_test
    async def test_consumer_management(self):
        nc = NATS()
        await nc.connect()
        jsm = nc.jsm()

        acc = await jsm.account_info()
        self.assertIsInstance(acc, nats.js.api.AccountInfo)

        # Create stream.
        await jsm.add_stream(name="ctests", subjects=["a", "b", "c.>"])

        # Create durable consumer.
        cinfo = await jsm.add_consumer(
            "ctests",
            durable_name="dur",
            ack_policy="explicit",
        )

        # Fail with missing stream.
        with self.assertRaises(NotFoundError) as err:
            await jsm.consumer_info("missing", "c")
        self.assertEqual(err.exception.err_code, 10059)

        # Get consumer, there should be no changes.
        current = await jsm.consumer_info("ctests", "dur")
        self.assertEqual(cinfo, current)

        # Delete consumer.
        ok = await jsm.delete_consumer("ctests", "dur")
        self.assertTrue(ok)

        # Consumer lookup should not be 404 now.
        with self.assertRaises(NotFoundError) as err:
            await jsm.consumer_info("ctests", "dur")
        self.assertEqual(err.exception.err_code, 10014)

        # Create ephemeral consumer.
        cinfo = await jsm.add_consumer(
            "ctests",
            ack_policy="explicit",
            deliver_subject="asdf",
        )
        # Should not be empty.
        self.assertTrue(len(cinfo.name) > 0)
        ok = await jsm.delete_consumer("ctests", cinfo.name)
        self.assertTrue(ok)

        # Ephemeral no longer found after delete.
        with self.assertRaises(NotFoundError):
            await jsm.delete_consumer("ctests", cinfo.name)

        await nc.close()


class SubscribeTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_queue_subscribe_deliver_group(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.add_stream(name="qsub", subjects=["quux"])

        a, b, c = ([], [], [])

        async def cb1(msg):
            a.append(msg)

        async def cb2(msg):
            b.append(msg)

        async def cb3(msg):
            c.append(msg)

        subs = []

        # First queue subscriber will create.
        sub1 = await js.subscribe("quux", "wg", cb=cb1)

        # Rest of queue subscribers will bind to the same.
        sub2 = await js.subscribe("quux", "wg", cb=cb2)
        sub3 = await js.subscribe("quux", "wg", cb=cb3)

        # All should be bound to the same subject.
        self.assertEqual(sub1.subject, sub2.subject)
        self.assertEqual(sub1.subject, sub3.subject)

        subs.append(sub1)
        subs.append(sub2)
        subs.append(sub3)

        for i in range(100):
            await js.publish("quux", f'Hello World {i}'.encode())

        delivered = [a, b, c]
        for i in range(5):
            await asyncio.sleep(0.5)
            total = len(a) + len(b) + len(c)
            if total == 100:
                break

        # Check that there was a good balance among the group members.
        self.assertEqual(len(a) + len(b) + len(c), 100)
        for q in delivered:
            self.assertTrue(10 <= len(q) <= 70)

        # Now unsubscribe all.
        await sub1.unsubscribe()
        await sub2.drain()
        await sub3.unsubscribe()

        # Confirm that no more messages are received.
        for i in range(200, 210):
            await js.publish("quux", f'Hello World {i}'.encode())

        with self.assertRaises(BadSubscriptionError):
            await sub1.unsubscribe()

        with self.assertRaises(BadSubscriptionError):
            await sub2.drain()

        await nc.close()

    @async_test
    async def test_subscribe_push_bound(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.add_stream(name="pbound", subjects=["pbound"])

        a, b = ([], [])

        async def cb1(msg):
            a.append(msg)

        async def cb2(msg):
            b.append(msg)

        subs = []

        for i in range(10):
            await js.publish("pbound", f'Hello World {i}'.encode())

        # First subscriber will create.
        sub1 = await js.subscribe("pbound", cb=cb1, durable="singleton")
        await asyncio.sleep(0.5)

        info = await js.consumer_info("pbound", "singleton")
        self.assertTrue(info.push_bound)

        # Rest of subscribers will not bind because it is already bound.
        with self.assertRaises(nats.js.errors.Error) as err:
            await js.subscribe("pbound", cb=cb2, durable="singleton")
        self.assertEqual(
            err.exception.description,
            "consumer is already bound to a subscription"
        )

        with self.assertRaises(nats.js.errors.Error) as err:
            await js.subscribe(
                "pbound", queue="foo", cb=cb2, durable="singleton"
            )
        self.assertEqual(
            err.exception.description,
            "cannot create queue subscription 'foo' to consumer 'singleton'"
        )

        # Wait the delivery of the messages.
        for i in range(5):
            if len(a) == 10:
                break
            await asyncio.sleep(0.2)
        self.assertEqual(len(a), 10)

        # Create a sync subscriber now.
        sub2 = await js.subscribe("pbound", durable="two")
        msg = await sub2.next_msg()
        self.assertEqual(msg.data, b'Hello World 0')
        self.assertEqual(msg.metadata.sequence.stream, 1)
        self.assertEqual(msg.metadata.sequence.consumer, 1)
        self.assertEqual(sub2.pending_msgs, 9)

        await nc.close()

    @async_test
    async def test_ephemeral_subscribe(self):
        nc = await nats.connect()
        js = nc.jetstream()

        subject = "ephemeral"
        await js.add_stream(name=subject, subjects=[subject])

        for i in range(10):
            await js.publish(subject, f'Hello World {i}'.encode())

        # First subscriber will create.
        sub1 = await js.subscribe(subject)
        sub2 = await js.subscribe(subject)

        recvd = 0
        async for msg in sub1.messages:
            recvd += 1
            await msg.ack_sync()

            if recvd == 10:
                break

        # Both should be able to process the messages at their own pace.
        self.assertEqual(sub1.pending_msgs, 0)
        self.assertEqual(sub2.pending_msgs, 10)

        info1 = await sub1.consumer_info()
        self.assertEqual(info1.stream_name, "ephemeral")
        self.assertEqual(info1.num_ack_pending, 0)
        self.assertTrue(len(info1.name) > 0)

        info2 = await sub2.consumer_info()
        self.assertEqual(info2.stream_name, "ephemeral")
        self.assertEqual(info2.num_ack_pending, 10)
        self.assertTrue(len(info2.name) > 0)
        self.assertTrue(info1.name != info2.name)


if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
