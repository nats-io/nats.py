import unittest

import nats
from nats.aio.client import __version__
from nats.aio.errors import *
from nats.aio.js.models.streams import Mirror, Source
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
        nc = await nats.connect()

        js = nc.jetstream()
        info = await js.account_info()
        self.assertTrue(isinstance(info.limits.max_streams, int))
        self.assertTrue(isinstance(info.limits.max_consumers, int))
        self.assertTrue(isinstance(info.limits.max_storage, int))
        self.assertTrue(isinstance(info.limits.max_memory, int))

        await nc.close()

    @async_test
    async def test_add_stream(self):
        nc = await nats.connect()

        js = nc.jetstream()

        await js.stream.create("TEST")
        info = await js.account_info()
        self.assertTrue(info.streams >= 1)

        await js.stream.create("TEST_mirror", mirror=Mirror("TEST"))
        info = await js.stream.info("TEST_mirror")
        self.assertTrue(info.mirror.name == "TEST")
        self.assertTrue(info.config.mirror.name == "TEST")
        await js.stream.create("TEST_sources", sources=[Source(name="TEST")])
        info = await js.stream.info("TEST_sources")
        self.assertTrue(len(info.config.sources) == 1)
        self.assertTrue(len(info.sources) == 1)

        await nc.close()

    @async_test
    async def test_pull_next(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.create("TEST", subjects=["foo", "bar"])

        for i in range(0, 100):
            await nc.publish("foo", f'msg:{i}'.encode())

        msg = await js.consumer.pull_next(
            "dur", subject="foo", deliver_policy="all", auto_ack=False
        )
        self.assertEqual(len(msg.data), 5)

        # Get the consumer info from the consumer.
        result = await js.consumer.info("TEST", "dur")

        # Have pulled only one message, inflight acks is 1.
        self.assertEqual(result.num_ack_pending, 1)
        # Messages waiting to be consumed.
        self.assertEqual(result.num_pending, 99)

        await msg.respond(b"")

        result = await js.consumer.info("TEST", "dur")
        self.assertEqual(result.num_ack_pending, 0)
        self.assertEqual(result.num_pending, 99)

        await nc.close()


if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
