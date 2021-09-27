import unittest

import nats
from nats.aio.client import __version__
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


if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
