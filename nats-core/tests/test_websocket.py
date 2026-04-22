"""Tests for WebSocket transport in NATS client."""

import asyncio
import os
import ssl

import pytest
from nats.client import connect
from nats.server import run


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_wss_connect_and_publish_subscribe():
    """Client connects over wss:// and exchanges messages over TLS."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket_tls.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        wss_url = server.websocket_url.replace("ws://", "wss://", 1)

        ssl_context = ssl.create_default_context(cafile=os.path.join(os.path.dirname(__file__), "certs", "ca.pem"))
        ssl_context.check_hostname = False

        client = await connect(wss_url, tls=ssl_context, timeout=2.0)
        try:
            subscription = await client.subscribe("wss.test")
            await client.publish("wss.test", b"hello over wss")
            message = await asyncio.wait_for(subscription.next(), timeout=2.0)
            assert message.data == b"hello over wss"
        finally:
            await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_ws_scheme_with_tls_context_promotes_to_wss():
    """A ws:// URL plus a TLS context is promoted to wss:// (matches nats.go semantics)."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_websocket_tls.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        assert server.websocket_url is not None
        # Intentionally keep the ws:// scheme; the TLS context should promote to wss://
        ws_url = server.websocket_url

        ssl_context = ssl.create_default_context(cafile=os.path.join(os.path.dirname(__file__), "certs", "ca.pem"))
        ssl_context.check_hostname = False

        client = await connect(ws_url, tls=ssl_context, timeout=2.0)
        try:
            await client.flush()
        finally:
            await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
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
