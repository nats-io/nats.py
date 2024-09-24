import asyncio
import unittest

import nats
import pytest
from nats.aio.errors import *
from tests.utils import *

try:
    import aiohttp
    aiohttp_installed = True
except ModuleNotFoundError:
    aiohttp_installed = False


class WebSocketTest(SingleWebSocketServerTestCase):

    @async_test
    async def test_simple_headers(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        nc = await nats.connect("ws://localhost:8080")

        sub = await nc.subscribe("foo")
        await nc.flush()
        await nc.publish(
            "foo", b"hello world", headers={
                "foo": "bar",
                "hello": "world-1"
            }
        )

        msg = await sub.next_msg()
        self.assertTrue(msg.headers != None)
        self.assertEqual(len(msg.headers), 2)

        self.assertEqual(msg.headers["foo"], "bar")
        self.assertEqual(msg.headers["hello"], "world-1")

        await nc.close()

    @async_test
    async def test_request_with_headers(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        nc = await nats.connect("ws://localhost:8080")

        async def service(msg):
            # Add another header
            msg.headers["quux"] = "quuz"
            await msg.respond(b"OK!")

        await nc.subscribe("foo", cb=service)
        await nc.flush()
        msg = await nc.request(
            "foo", b"hello world", headers={
                "foo": "bar",
                "hello": "world"
            }
        )

        self.assertTrue(msg.headers != None)
        self.assertEqual(len(msg.headers), 3)
        self.assertEqual(msg.headers["foo"], "bar")
        self.assertEqual(msg.headers["hello"], "world")
        self.assertEqual(msg.headers["quux"], "quuz")
        self.assertEqual(msg.data, b"OK!")

        await nc.close()

    @async_test
    async def test_empty_headers(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        nc = await nats.connect("ws://localhost:8080")

        sub = await nc.subscribe("foo")
        await nc.flush()
        await nc.publish("foo", b"hello world", headers={"": ""})

        msg = await sub.next_msg()
        self.assertTrue(msg.headers == None)

        # Empty long key
        await nc.publish("foo", b"hello world", headers={"      ": ""})
        msg = await sub.next_msg()
        self.assertTrue(msg.headers == None)

        # Empty long key
        await nc.publish(
            "foo", b"hello world", headers={"": "                  "}
        )
        msg = await sub.next_msg()
        self.assertTrue(msg.headers == None)

        hdrs = {
            "timestamp": "2022-06-15T19:08:14.639020",
            "type": "rpc",
            "command": "publish_state",
            "trace_id": "",
            "span_id": "",
        }
        await nc.publish("foo", b"Hello from Python!", headers=hdrs)
        msg = await sub.next_msg()
        self.assertEqual(msg.headers, hdrs)

        await nc.close()

    @async_test
    async def test_reconnect(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        reconnected = asyncio.Future()

        async def reconnected_cb():
            if not reconnected.done():
                reconnected.set_result(True)

        nc = await nats.connect(
            "ws://localhost:8080",
            reconnected_cb=reconnected_cb,
        )

        sub = await nc.subscribe("foo")

        async def bar_cb(msg):
            await msg.respond(b"OK!")

        rsub = await nc.subscribe("bar", cb=bar_cb)
        await nc.publish("foo", b"First")
        await nc.flush()
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"First")

        rmsg = await nc.request("bar", b"hi")
        self.assertEqual(rmsg.data, b"OK!")

        # Restart the server and wait for reconnect.
        await asyncio.get_running_loop().run_in_executor(
            None, self.server_pool[0].stop
        )
        await asyncio.sleep(1)
        await asyncio.get_running_loop().run_in_executor(
            None, self.server_pool[0].start
        )
        await asyncio.wait_for(reconnected, 2)

        # Get another message.
        await nc.publish("foo", b"Second")
        await nc.flush()
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"Second")
        rmsg = await nc.request("bar", b"hi")
        self.assertEqual(rmsg.data, b"OK!")

        await nc.close()

    @async_test
    async def test_close_while_disconnected(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        reconnected = asyncio.Future()

        async def reconnected_cb():
            if not reconnected.done():
                reconnected.set_result(True)

        nc = await nats.connect(
            "ws://localhost:8080",
            reconnected_cb=reconnected_cb,
        )

        # Create both sync and async subscriptions.
        sub = await nc.subscribe("foo")

        async def bar_cb(msg):
            await msg.respond(b"OK!")

        rsub = await nc.subscribe("bar", cb=bar_cb)
        await nc.publish("foo", b"First")
        await nc.flush()
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"First")
        rmsg = await nc.request("bar", b"hi")
        self.assertEqual(rmsg.data, b"OK!")

        # Restart the server and wait for reconnect.
        await asyncio.get_running_loop().run_in_executor(
            None, self.server_pool[0].stop
        )
        await asyncio.sleep(1)

        # Should not fail closing while disconnected.
        await nc.close()


class WebSocketTLSTest(SingleWebSocketTLSServerTestCase):

    @async_test
    async def test_pub_sub(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        nc = await nats.connect("wss://localhost:8081", tls=self.ssl_ctx)

        sub = await nc.subscribe("foo")
        await nc.flush()
        await nc.publish("foo", b"hello world", headers={"foo": "bar"})

        msg = await sub.next_msg()
        self.assertTrue(msg.headers != None)
        self.assertEqual(len(msg.headers), 1)

        self.assertEqual(msg.headers["foo"], "bar")

        await nc.close()

    @async_test
    async def test_reconnect(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        reconnected = asyncio.Future()

        async def reconnected_cb():
            if not reconnected.done():
                reconnected.set_result(True)

        nc = await nats.connect(
            "wss://localhost:8081",
            reconnected_cb=reconnected_cb,
            tls=self.ssl_ctx
        )

        sub = await nc.subscribe("foo")

        async def bar_cb(msg):
            await msg.respond(b"OK!")

        rsub = await nc.subscribe("bar", cb=bar_cb)
        await nc.publish("foo", b"First")
        await nc.flush()
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"First")

        rmsg = await nc.request("bar", b"hi")
        self.assertEqual(rmsg.data, b"OK!")

        # Restart the server and wait for reconnect.
        await asyncio.get_running_loop().run_in_executor(
            None, self.server_pool[0].stop
        )
        await asyncio.sleep(1)
        await asyncio.get_running_loop().run_in_executor(
            None, self.server_pool[0].start
        )
        await asyncio.wait_for(reconnected, 2)

        # Get another message.
        await nc.publish("foo", b"Second")
        await nc.flush()
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"Second")
        rmsg = await nc.request("bar", b"hi")
        self.assertEqual(rmsg.data, b"OK!")

        await nc.close()

    @async_test
    async def test_close_while_disconnected(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        reconnected = asyncio.Future()

        async def reconnected_cb():
            if not reconnected.done():
                reconnected.set_result(True)

        nc = await nats.connect(
            "wss://localhost:8081",
            reconnected_cb=reconnected_cb,
            tls=self.ssl_ctx,
        )

        # Create both sync and async subscriptions.
        sub = await nc.subscribe("foo")

        async def bar_cb(msg):
            await msg.respond(b"OK!")

        rsub = await nc.subscribe("bar", cb=bar_cb)
        await nc.publish("foo", b"First")
        await nc.flush()
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"First")
        rmsg = await nc.request("bar", b"hi")
        self.assertEqual(rmsg.data, b"OK!")

        # Restart the server and wait for reconnect.
        await asyncio.get_running_loop().run_in_executor(
            None, self.server_pool[0].stop
        )
        await asyncio.sleep(1)

        # Should not fail closing while disconnected.
        await nc.close()


if __name__ == "__main__":
    import sys

    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
