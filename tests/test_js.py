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
from nats.aio.client import Client as NATS
from nats.aio.client import __version__
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

        await js._jsm._add_stream(
            config={
                "name": "QUUX",
                "subjects": ["quux"],
            }
        )

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
    async def test_fetch_one(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        # TODO: js.add_stream()
        await js._jsm._add_stream(
            config={
                "name": "TEST",
                "subjects": ["foo", "bar"],
            }
        )

        # TODO: js.add_consumer()
        await js._jsm._add_consumer(
            stream="TEST",
            durable="dur",
            ack_policy="explicit"
        )

        ack = await js.publish("foo", f'Hello from NATS!'.encode())
        self.assertEqual(ack.stream, "TEST")
        self.assertEqual(ack.seq, 1)

        sub = await js.pull_subscribe("foo", "dur")
        msgs = await sub.fetch(1)
        for msg in msgs:
            await msg.ack()

        msg = msgs[0]
        self.assertEqual(msg.metadata.sequence.stream, 1)
        self.assertEqual(msg.metadata.sequence.consumer, 1)
        self.assertTrue(datetime.datetime.now() > msg.metadata.timestamp)
        self.assertEqual(msg.metadata.num_pending, 0)
        self.assertEqual(msg.metadata.num_delivered, 1)

        await nc.close()

if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
