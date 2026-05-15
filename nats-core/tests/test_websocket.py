"""Tests for WebSocket transport in NATS client."""

import asyncio
import os
import ssl
import tempfile

import pytest
from nats.client import connect
from nats.server import run


async def test_connect_ws_succeeds():
    """Client connects over ws:// and exchanges INFO/CONNECT."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        client = await connect(server.websocket_url, timeout=2.0)
        try:
            await client.flush()
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_ws_publish_subscribe_roundtrip():
    """Messages flow over the WebSocket transport end-to-end."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        client = await connect(server.websocket_url, timeout=2.0)
        try:
            subscription = await client.subscribe("ws.test")
            await client.publish("ws.test", b"hello over ws")
            message = await asyncio.wait_for(subscription.next(), timeout=2.0)
            assert message.data == b"hello over ws"
            assert message.subject == "ws.test"
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_wss_connect_and_publish_subscribe():
    """Client connects over wss:// and exchanges messages over TLS."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket_tls.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        assert server.websocket_url.startswith("wss://")

        ssl_context = ssl.create_default_context(cafile=os.path.join(os.path.dirname(__file__), "certs", "ca.pem"))
        ssl_context.check_hostname = False

        client = await connect(server.websocket_url, tls=ssl_context, timeout=2.0)
        try:
            subscription = await client.subscribe("wss.test")
            await client.publish("wss.test", b"hello over wss")
            message = await asyncio.wait_for(subscription.next(), timeout=2.0)
            assert message.data == b"hello over wss"
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_ws_scheme_with_tls_context_promotes_to_wss():
    """A ws:// URL plus a TLS context is promoted to wss:// (matches nats.go semantics)."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket_tls.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        # Override the wss:// scheme with ws://; the TLS context should still upgrade.
        ws_url = server.websocket_url.replace("wss://", "ws://", 1)

        ssl_context = ssl.create_default_context(cafile=os.path.join(os.path.dirname(__file__), "certs", "ca.pem"))
        ssl_context.check_hostname = False

        client = await connect(ws_url, tls=ssl_context, timeout=2.0)
        try:
            await client.flush()
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_ws_url_preserved_in_server_pool():
    """The seed entry in the reconnect pool keeps the ws:// scheme."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        client = await connect(server.websocket_url, timeout=2.0)
        try:
            # TODO: replace with a public servers accessor once one is exposed.
            assert client._server_pool[0].startswith(("ws://", "wss://"))
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_wss_url_preserved_in_server_pool():
    """A ws:// URL upgraded to wss:// keeps the wss:// scheme in the reconnect pool."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket_tls.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        ws_url = server.websocket_url.replace("wss://", "ws://", 1)

        ssl_context = ssl.create_default_context(cafile=os.path.join(os.path.dirname(__file__), "certs", "ca.pem"))
        ssl_context.check_hostname = False

        client = await connect(ws_url, tls=ssl_context, timeout=2.0)
        try:
            # TODO: replace with a public servers accessor once one is exposed.
            assert client._server_pool[0].startswith("wss://")
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_wss_native_url_preserved_in_server_pool():
    """A native wss:// URL (no upgrade) keeps the wss:// scheme in the reconnect pool."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket_tls.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        assert server.websocket_url.startswith("wss://")

        ssl_context = ssl.create_default_context(cafile=os.path.join(os.path.dirname(__file__), "certs", "ca.pem"))
        ssl_context.check_hostname = False

        client = await connect(server.websocket_url, tls=ssl_context, timeout=2.0)
        try:
            # TODO: replace with a public servers accessor once one is exposed.
            assert client._server_pool[0].startswith("wss://")
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_ws_headers_roundtrip():
    """Headers survive a publish/subscribe roundtrip over WebSocket (HMSG framing)."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        client = await connect(server.websocket_url, timeout=2.0)
        try:
            subscription = await client.subscribe("ws.hdrs")
            await client.publish("ws.hdrs", b"payload", headers={"X-Custom": "value"})
            message = await asyncio.wait_for(subscription.next(), timeout=2.0)
            assert message.data == b"payload"
            assert message.headers is not None
            assert message.headers.get("X-Custom") == "value"
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_ws_large_payload_spans_multiple_frames():
    """A payload larger than a typical WS frame round-trips correctly."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        client = await connect(server.websocket_url, timeout=2.0)
        try:
            payload = bytes(range(256)) * 800  # ~200 KB, non-trivial pattern
            subscription = await client.subscribe("ws.large")
            await client.publish("ws.large", payload)
            message = await asyncio.wait_for(subscription.next(), timeout=5.0)
            assert message.data == payload
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_ws_connect_fails_for_unreachable_server():
    """A ws:// URL pointing at a non-listening port raises an error."""
    with pytest.raises((ConnectionError, TimeoutError, OSError)):
        await connect("ws://127.0.0.1:1", timeout=0.5)


async def test_ws_reconnects_over_websocket():
    """After the WebSocket server is bounced, the client reopens over WebSocket — not TCP."""
    base_config = os.path.join(os.path.dirname(__file__), "configs", "server_websocket.conf")
    first_server = await run(config_path=base_config, port=0, timeout=5.0)

    temp_config = None
    second_server = None
    client = None
    try:
        assert first_server.websocket_url is not None
        ws_url = first_server.websocket_url
        ws_port = int(ws_url.rsplit(":", 1)[1])

        # Pin the websocket port for the restart so the client's seed URL still resolves.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(f"websocket {{\n  port: {ws_port}\n  no_tls: true\n}}\n")
            temp_config = f.name

        reconnected = asyncio.Event()
        client = await connect(
            ws_url,
            reconnect_time_wait=0.1,
            reconnect_max_attempts=50,
            timeout=2.0,
        )
        client.add_reconnected_callback(lambda: reconnected.set())

        await first_server.shutdown()
        first_server = None  # don't double-shutdown in finally

        second_server = await run(config_path=temp_config, port=0, timeout=5.0)

        await asyncio.wait_for(reconnected.wait(), timeout=10.0)

        subscription = await client.subscribe("ws.reconnect")
        await client.publish("ws.reconnect", b"after-reconnect")
        message = await asyncio.wait_for(subscription.next(), timeout=2.0)
        assert message.data == b"after-reconnect"
    finally:
        if client is not None:
            await client.close()
        if second_server is not None:
            await second_server.shutdown()
        if first_server is not None:
            await first_server.shutdown()
        if temp_config is not None:
            os.unlink(temp_config)


async def test_wss_uses_default_tls_context_when_unspecified():
    """A wss:// URL with no explicit tls= falls back to the system default SSLContext."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket_tls.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        assert server.websocket_url.startswith("wss://")

        # The test server uses a self-signed cert, so the default trust store should
        # reject it — proving that connect() did apply a default SSLContext for wss://.
        with pytest.raises((ConnectionError, ssl.SSLError, ssl.SSLCertVerificationError)):
            await connect(server.websocket_url, timeout=2.0)
    finally:
        await server.shutdown()


async def test_ws_with_permessage_deflate_compression():
    """Messages round-trip when the server negotiates permessage-deflate compression."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket_compression.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        client = await connect(server.websocket_url, timeout=2.0)
        try:
            payload = b"compressible payload " * 1000
            subscription = await client.subscribe("ws.compress")
            await client.publish("ws.compress", payload)
            message = await asyncio.wait_for(subscription.next(), timeout=5.0)
            assert message.data == payload
        finally:
            await client.close()
    finally:
        await server.shutdown()


async def test_ws_request_reply_roundtrip():
    """Request/reply works across the WebSocket transport."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        client = await connect(server.websocket_url, timeout=2.0)
        try:
            subscription = await client.subscribe("ws.echo")

            async def responder():
                message = await subscription.next()
                assert message.reply is not None
                await client.publish(message.reply, message.data)

            responder_task = asyncio.create_task(responder())
            try:
                reply = await client.request("ws.echo", b"ping", timeout=2.0)
                assert reply.data == b"ping"
            finally:
                await responder_task
        finally:
            await client.close()
    finally:
        await server.shutdown()
