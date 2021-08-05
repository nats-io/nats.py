import asyncio
import http.client
import json
import ssl
import time
import unittest
from unittest import mock
import tempfile
import shutil

import nats
from nats.aio.client import Client as NATS
from nats.aio.client import __version__
from nats.aio.utils import new_inbox, INBOX_PREFIX
from nats.aio.errors import *
from tests.utils import *

class HeadersTest(SingleServerTestCase):
    @async_test
    async def test_simple_headers(self):
        nc = await nats.connect()

        sub = await nc.subscribe("foo")
        await nc.flush()
        await nc.publish(
            "foo", b'hello world', headers={
                'foo': 'bar',
                'hello': 'world'
            }
        )

        msg = await sub.next_msg()
        self.assertTrue(msg.headers != None)
        self.assertEqual(len(msg.headers), 2)

        self.assertEqual(msg.headers['foo'], 'bar')
        self.assertEqual(msg.headers['hello'], 'world')

        await nc.close()

    @async_test
    async def test_request_with_headers(self):
        nc = await nats.connect()

        async def service(msg):
            # Add another header
            msg.headers['quux'] = 'quuz'
            await msg.respond(b'OK!')

        await nc.subscribe("foo", cb=service)
        await nc.flush()
        msg = await nc.request(
            "foo", b'hello world', headers={
                'foo': 'bar',
                'hello': 'world'
            }
        )

        self.assertTrue(msg.headers != None)
        self.assertEqual(len(msg.headers), 3)
        self.assertEqual(msg.headers['foo'], 'bar')
        self.assertEqual(msg.headers['hello'], 'world')
        self.assertEqual(msg.headers['quux'], 'quuz')
        self.assertEqual(msg.data, b'OK!')

        await nc.close()


class JetStreamTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_check_account_info(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()
        info = await js._jsm._account_info()
        self.assertTrue(len(info["limits"]) > 0)

        await nc.close()

    @async_test
    async def test_add_stream(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        result = await js._jsm._add_stream(config={"name": "TEST"})
        info = await js._jsm._account_info()
        self.assertTrue(info['streams'] >= 1)

        await nc.close()

    @async_test
    async def test_pull_subscribe_fetch_one(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()
        await js._jsm._add_stream(config={
            "name": "TEST",
            "subjects": ["foo", "bar"],
            })

        for i in range(0, 100):
            await nc.publish("foo", f'msg:{i}'.encode())

        sub = await js.pull_subscribe("foo", "dur")
        msgs = await sub.fetch(1)
        self.assertEqual(len(msgs), 1)

        # Get the consumer info from the consumer.
        result = await js._jsm._consumer_info(
            stream="TEST",
            consumer="dur",
            )

        # Have pulled only one message, inflight acks is 1.
        self.assertEqual(result['num_ack_pending'], 1)
        # Messages waiting to be consumed.
        self.assertEqual(result['num_pending'], 99)

        for msg in msgs:
            await msg.ack()

        result = await js._jsm._consumer_info(
            stream="TEST",
            consumer="dur",
            )
        self.assertEqual(result['num_ack_pending'], 0)
        self.assertEqual(result['num_pending'], 99)

        await nc.close()

    @async_test
    async def test_pull_subscribe_fetch_one_later(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()
        stream = "TEST"
        subject = "foo"
        durable = "dur"
        await js._jsm._add_stream(config={
            "name": stream,
            "subjects": [subject],
            })

        sub = await js.pull_subscribe(subject, durable)

        with self.assertRaises(asyncio.TimeoutError):
            msgs = await sub.fetch(1)
            msg = msgs[0]
            print(msg.headers)
            print(type(msg.headers))
            print(len(msg.headers))

        for i in range(0, 10):
            await nc.publish(subject, f'msg:{i}'.encode())

        # Retry now that messages have been published.
        msgs = await sub.fetch(1)
        for msg in msgs:
            await msg.ack()

        result = await js._jsm._consumer_info(
            stream=stream,
            consumer=durable,
            )
        self.assertEqual(result['num_ack_pending'], 0)
        self.assertEqual(result['num_pending'], 9)

        await nc.close()

    @async_test
    async def test_push_ephemeral_consumer(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        subject = "foo"
        await js._jsm._add_stream(config={"name": "TEST", "subjects":[subject]})

        for i in range(0, 10):
            await nc.publish(subject, f'msg:{i}'.encode())
        await nc.flush()

        sub = await js.subscribe(subject)
        msg = await sub.next_msg()
        m = msg.metadata()
        self.assertEqual(m.stream, "TEST")
        self.assertEqual(m.num_pending, 9)
        self.assertEqual(m.num_delivered, 1)
        ackack = await msg.ack_sync()
        self.assertTrue(ackack is not None)

        msg = await sub.next_msg()
        m = msg.metadata()
        self.assertEqual(m.stream, "TEST")
        self.assertEqual(m.num_pending, 8)
        self.assertEqual(m.num_delivered, 1)
        ackack = await msg.ack_sync()
        self.assertTrue(ackack is not None)

        await nc.close()

    @async_test
    async def test_non_js_msg(self):
        nc = NATS()
        await nc.connect()

        subject = "foo"
        sub = await nc.subscribe(subject)
        for i in range(0, 10):
            await nc.publish(subject, f'msg:{i}'.encode())

        msg = await sub.next_msg()
        with self.assertRaises(ErrNotJSMessage):
            msg.metadata()

        with self.assertRaises(ErrNotJSMessage):
            await msg.ack()

        await nc.close()

if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
