import asyncio
import ssl
import unittest
from unittest import mock
from urllib.parse import urlparse, urlsplit

import pytest
from nats.aio.client import Client, Srv
from nats.aio.errors import *
from nats.aio.transport import WebSocketTransport

import nats
from tests.utils import *

try:
    import aiohttp

    aiohttp_installed = True
except ModuleNotFoundError:
    aiohttp_installed = False


class WebSocketProxyOptionsTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    @async_test
    async def test_proxy_authentication_options_are_validated(self):
        with pytest.raises(ValueError, match="must be provided together"):
            await Client().connect("ws://localhost:8080", proxy_user="proxy-user")

        with pytest.raises(ValueError, match="requires a proxy URL"):
            await Client().connect(
                "ws://localhost:8080",
                proxy_user="proxy-user",
                proxy_password="proxy-password",
            )

    @async_test
    async def test_client_forwards_proxy_options_to_transport(self):
        nc = Client()
        nc.options.update(
            {
                "connect_timeout": 2,
                "proxy": "http://proxy.example:8080",
                "proxy_user": "proxy-user",
                "proxy_password": "proxy-password",
                "ws_connection_headers": {"X-Test": ["value"]},
            }
        )
        server = Srv(urlparse("ws://localhost:8080"))
        transport = mock.Mock()
        transport.connect = mock.AsyncMock()

        with mock.patch("nats.aio.client.WebSocketTransport", return_value=transport) as transport_type:
            await nc._connect_to_server(server)

        transport_type.assert_called_once_with(
            ws_headers={"X-Test": ["value"]},
            proxy="http://proxy.example:8080",
            proxy_user="proxy-user",
            proxy_password="proxy-password",
        )
        transport.connect.assert_awaited_once()

    @async_test
    async def test_transport_forwards_proxy_options_for_ws_and_wss(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        session = mock.Mock()
        session.ws_connect = mock.AsyncMock(return_value=None)
        with mock.patch("nats.aio.transport.aiohttp.ClientSession", return_value=session):
            transport = WebSocketTransport(
                ws_headers={"X-Test": ["value"]},
                proxy="http://proxy.example:8080",
                proxy_user="proxy-user",
                proxy_password="proxy-password",
            )

        await transport.connect(urlparse("ws://localhost:8080"), buffer_size=0, connect_timeout=2)
        ssl_context = ssl.create_default_context()
        await transport.connect_tls(
            urlparse("wss://localhost:8081"),
            ssl_context=ssl_context,
            buffer_size=0,
            connect_timeout=3,
        )

        ws_call, wss_call = session.ws_connect.await_args_list
        assert ws_call.kwargs["proxy"] == "http://proxy.example:8080"
        assert ws_call.kwargs["proxy_auth"].login == "proxy-user"
        assert ws_call.kwargs["proxy_auth"].password == "proxy-password"
        assert ws_call.kwargs["headers"].getall("X-Test") == ["value"]
        assert ws_call.kwargs["max_msg_size"] == 0
        assert wss_call.kwargs["proxy"] == "http://proxy.example:8080"
        assert wss_call.kwargs["proxy_auth"].login == "proxy-user"
        assert wss_call.kwargs["proxy_auth"].password == "proxy-password"
        assert wss_call.kwargs["ssl"] is ssl_context
        assert wss_call.kwargs["headers"].getall("X-Test") == ["value"]
        assert wss_call.kwargs["max_msg_size"] == 0


class WebSocketTest(SingleWebSocketServerTestCase):
    @async_test
    async def test_connect_through_authenticated_proxy(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        proxy_headers = {}
        connections = set()

        async def relay(reader, writer):
            try:
                while True:
                    data = await reader.read(64 * 1024)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
            finally:
                writer.close()

        async def proxy_connection(client_reader, client_writer):
            try:
                request = await client_reader.readuntil(b"\r\n\r\n")
                lines = request.decode("latin1").split("\r\n")
                method, target, version = lines[0].split(" ", 2)
                target_url = urlsplit(target)
                path = target_url.path or "/"
                if target_url.query:
                    path = f"{path}?{target_url.query}"

                forwarded_headers = []
                for line in lines[1:]:
                    if not line:
                        continue
                    name, _, value = line.partition(":")
                    proxy_headers[name.lower()] = value.strip()
                    if name.lower() not in ("proxy-authorization", "proxy-connection"):
                        forwarded_headers.append(line)

                upstream_reader, upstream_writer = await asyncio.open_connection(
                    target_url.hostname,
                    target_url.port,
                )
                upstream_writer.write(
                    (f"{method} {path} {version}\r\n" + "\r\n".join(forwarded_headers) + "\r\n\r\n").encode("latin1")
                )
                await upstream_writer.drain()
                await asyncio.gather(
                    relay(client_reader, upstream_writer),
                    relay(upstream_reader, client_writer),
                )
            finally:
                client_writer.close()

        def accept_proxy_connection(reader, writer):
            task = asyncio.create_task(proxy_connection(reader, writer))
            connections.add(task)
            task.add_done_callback(connections.discard)

        proxy_server = await asyncio.start_server(accept_proxy_connection, "127.0.0.1", 0)
        proxy_port = proxy_server.sockets[0].getsockname()[1]
        nc = None
        try:
            nc = await nats.connect(
                "ws://localhost:8080",
                proxy=f"http://127.0.0.1:{proxy_port}",
                proxy_user="proxy-user",
                proxy_password="proxy-password",
            )
            sub = await nc.subscribe("proxy.test")
            await nc.publish("proxy.test", b"through proxy")
            await nc.flush()
            msg = await sub.next_msg(timeout=1)
            assert msg.data == b"through proxy"
            assert proxy_headers["proxy-authorization"] == "Basic cHJveHktdXNlcjpwcm94eS1wYXNzd29yZA=="
        finally:
            if nc is not None:
                await nc.close()
            proxy_server.close()
            await proxy_server.wait_closed()
            for task in tuple(connections):
                task.cancel()
            if connections:
                await asyncio.gather(*connections, return_exceptions=True)

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
