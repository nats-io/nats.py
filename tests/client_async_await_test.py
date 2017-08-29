import sys
import asyncio
import unittest

from nats.aio.client import Client as NATS
from tests.utils import (async_test, SingleServerTestCase)


class ClientAsyncAwaitTest(SingleServerTestCase):

    @async_test
    def test_async_await_subscribe_async(self):
        nc = NATS()
        msgs = []

        async def subscription_handler(msg):
            if msg.subject == "tests.1":
                await asyncio.sleep(0.5, loop=self.loop)
            if msg.subject == "tests.3":
                await asyncio.sleep(0.2, loop=self.loop)
            msgs.append(msg)

        yield from nc.connect(io_loop=self.loop)
        sid = yield from nc.subscribe("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield from nc.publish("tests.{}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield from asyncio.sleep(1, loop=self.loop)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield from nc.close()

    @async_test
    def test_async_await_subscribe_sync(self):
        nc = NATS()
        msgs = []

        async def subscription_handler(msg):
            if msg.subject == "tests.1":
                await asyncio.sleep(0.5, loop=self.loop)
            if msg.subject == "tests.3":
                await asyncio.sleep(0.2, loop=self.loop)
            msgs.append(msg)

        yield from nc.connect(io_loop=self.loop)
        sid = yield from nc.subscribe_async("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield from nc.publish("tests.{}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield from asyncio.sleep(1, loop=self.loop)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[4].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield from nc.close()


if __name__ == '__main__':
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
