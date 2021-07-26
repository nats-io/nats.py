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
from nats.aio.errors import ErrConnectionClosed, ErrNoServers, ErrTimeout, \
    ErrBadSubject, ErrConnectionDraining, ErrDrainTimeout, NatsError, ErrInvalidCallbackType
from nats.aio.utils import new_inbox, INBOX_PREFIX
from tests.utils import async_test, SingleServerTestCase, MultiServerAuthTestCase, MultiServerAuthTokenTestCase, \
    TLSServerTestCase, \
    MultiTLSServerAuthTestCase, ClusteringTestCase, ClusteringDiscoveryAuthTestCase, SingleJetStreamServerTestCase


class NATS22Test(SingleServerTestCase):
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
        info = await js.account_info()
        self.assertTrue(len(info["limits"]) > 0)

        await nc.close()

    @async_test
    async def test_add_stream(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        result = await js.add_stream(config={"name": "TEST"})
        info = await js.account_info()
        self.assertTrue(info['streams'] >= 1)

        await nc.close()

    @async_test
    async def test_add_consumer(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(config={"name": "foo"})

        await js.add_consumer(
            stream_name="foo",
            config={
                "durable_name": "bar",
                "ack_policy": "explicit"
            }
        )
        await nc.publish("foo", b'Hello JS!')
        await nc.flush()

        msg = await nc.request(
            "$JS.API.CONSUMER.MSG.NEXT.foo.bar", timeout=1, old_style=True
        )
        info = await js.account_info()
        self.assertTrue(info["consumers"] >= 1)
        await nc.close()

    @async_test
    async def test_add_consumer(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(config={"name": "foo"})

        await js.add_consumer(
            stream_name="foo",
            config={
                "durable_name": "bar",
                "ack_policy": "explicit"
            }
        )
        await nc.publish("foo", b'Hello JS!')
        await nc.flush()

        msg = await nc.request(
            "$JS.API.CONSUMER.MSG.NEXT.foo.bar", timeout=2, old_style=True
        )
        info = await js.account_info()
        self.assertTrue(info["consumers"] >= 1)
        await nc.close()


if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
