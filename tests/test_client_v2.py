import asyncio
import http.client
import json
import shutil
import ssl
import tempfile
import time
import unittest
from unittest import mock

import nats
from nats.aio.client import Client as NATS, __version__
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
                'hello': 'world-1'
            }
        )

        msg = await sub.next_msg()
        self.assertTrue(msg.headers != None)
        self.assertEqual(len(msg.headers), 2)

        self.assertEqual(msg.headers['foo'], 'bar')
        self.assertEqual(msg.headers['hello'], 'world-1')

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

    @async_test
    async def test_empty_headers(self):
        nc = await nats.connect()

        sub = await nc.subscribe("foo")
        await nc.flush()
        await nc.publish("foo", b'hello world', headers={'': ''})

        msg = await sub.next_msg()
        self.assertTrue(msg.headers == None)

        # Empty long key
        await nc.publish("foo", b'hello world', headers={'      ': ''})
        msg = await sub.next_msg()
        self.assertTrue(msg.headers == None)

        # Empty long key
        await nc.publish(
            "foo", b'hello world', headers={'': '                  '}
        )
        msg = await sub.next_msg()
        self.assertTrue(msg.headers == None)

        hdrs = {
            'timestamp': '2022-06-15T19:08:14.639020',
            'type': 'rpc',
            'command': 'publish_state',
            'trace_id': '',
            'span_id': ''
        }
        await nc.publish("foo", b'Hello from Python!', headers=hdrs)
        msg = await sub.next_msg()
        self.assertEqual(msg.headers, hdrs)

        await nc.close()


if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
