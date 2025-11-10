import asyncio
import unittest

import pytest
from nats.aio.errors import *

import nats
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
        await nc.publish("foo", b"hello world", headers={"foo": "bar", "hello": "world-1"})

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
        msg = await nc.request("foo", b"hello world", headers={"foo": "bar", "hello": "world"})

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
        await nc.publish("foo", b"hello world", headers={"": "                  "})
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
        await asyncio.get_running_loop().run_in_executor(None, self.server_pool[0].stop)
        await asyncio.sleep(1)
        await asyncio.get_running_loop().run_in_executor(None, self.server_pool[0].start)
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
        await asyncio.get_running_loop().run_in_executor(None, self.server_pool[0].stop)
        await asyncio.sleep(1)

        # Should not fail closing while disconnected.
        await nc.close()

    @async_test
    async def test_with_static_headers(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        custom_headers = {
            "Authorization": ["Bearer RandomToken"],
            "X-Client-ID": ["test-client-123"],
            "X-Custom-Header": ["custom-value"],
            "Accept": ["application/json", "text/plain", "application/msgpack"],
            "X-Feature-Flags": ["feature-a", "feature-b", "feature-c"],
            "X-Capabilities": ["streaming", "compression", "batching"],
        }

        nc = await nats.connect("ws://localhost:8080", ws_connection_headers=custom_headers)

        # Test basic pub/sub functionality to ensure connection works
        sub = await nc.subscribe("foo")
        await nc.flush()

        # Create test messages
        msgs = []
        for i in range(10):
            msg = b"A" * 100  # 100 bytes of 'A'
            msgs.append(msg)

        # Publish messages
        for i, msg in enumerate(msgs):
            await nc.publish("foo", msg)
            # Ensure message content is not modified
            assert msg == msgs[i], "User content was changed during publish"

        # Receive and verify messages
        for i in range(len(msgs)):
            msg = await sub.next_msg(timeout=1.0)
            assert msg.data == msgs[i], f"Expected message {i}: {msgs[i]}, got {msg.data}"

        await nc.close()

    @async_test
    async def test_ws_headers_with_reconnect(self):
        """Test that headers persist across reconnections"""
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        reconnect_count = 0
        reconnected = asyncio.Future()

        async def reconnected_cb():
            nonlocal reconnect_count
            reconnect_count += 1
            if not reconnected.done():
                reconnected.set_result(True)

        # Connect with custom headers
        custom_headers = {"X-Persistent-Session": ["session-12345"], "Authorization": ["Bearer ReconnectToken"]}

        nc = await nats.connect(
            "ws://localhost:8080",
            ws_connection_headers=custom_headers,
            reconnected_cb=reconnected_cb,
            max_reconnect_attempts=5,
        )

        # Create subscription
        messages_received = []

        async def message_handler(msg):
            messages_received.append(msg.data)

        await nc.subscribe("reconnect.test", cb=message_handler)

        # Publish before reconnect
        await nc.publish("reconnect.test", b"Before reconnect")
        await nc.flush()

        # Simulate server restart
        await asyncio.get_running_loop().run_in_executor(None, self.server_pool[0].stop)
        await asyncio.sleep(1)
        await asyncio.get_running_loop().run_in_executor(None, self.server_pool[0].start)

        # Wait for reconnection
        await asyncio.wait_for(reconnected, timeout=5.0)

        # Publish after reconnect
        await nc.publish("reconnect.test", b"After reconnect")
        await nc.flush()

        # Wait a bit for message delivery
        await asyncio.sleep(0.5)

        # Verify we got messages
        assert b"Before reconnect" in messages_received
        assert b"After reconnect" in messages_received
        assert reconnect_count > 0

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

        nc = await nats.connect("wss://localhost:8081", reconnected_cb=reconnected_cb, tls=self.ssl_ctx)

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
        await asyncio.get_running_loop().run_in_executor(None, self.server_pool[0].stop)
        await asyncio.sleep(1)
        await asyncio.get_running_loop().run_in_executor(None, self.server_pool[0].start)
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
        await asyncio.get_running_loop().run_in_executor(None, self.server_pool[0].stop)
        await asyncio.sleep(1)

        # Should not fail closing while disconnected.
        await nc.close()

    @async_test
    async def test_ws_headers_with_tls(self):
        """Test custom headers with TLS WebSocket connection"""
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        # Note: This would require a TLS-enabled test server
        # Keeping structure similar to the non-TLS test
        custom_headers = {"Authorization": ["Bearer SecureToken"], "X-TLS-Client": ["secure-client-v1"]}

        nc = await nats.connect("wss://localhost:8081", ws_connection_headers=custom_headers, tls=self.ssl_ctx)

        # Basic functionality test
        sub = await nc.subscribe("tls.test")
        await nc.publish("tls.test", b"TLS test message")

        msg = await sub.next_msg(timeout=1.0)
        assert msg.data == b"TLS test message"

        await nc.close()


if __name__ == "__main__":
    import sys

    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
