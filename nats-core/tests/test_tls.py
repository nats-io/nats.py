"""Tests for TLS functionality in NATS client."""

import asyncio
import contextlib
import os
import ssl
from pathlib import Path

import pytest
from nats.client import ClientStatus, connect
from nats.client.errors import SecureConnectionRequiredError
from nats.server import Server, run


@pytest.mark.asyncio
async def test_tls_handshake_first_with_custom_ssl_context():
    """Test TLS connection with handshake first mode using custom SSL context."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_tls_handshake_first.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Create SSL context that trusts our self-signed certificate
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Connect with TLS handshake first
        client = await connect(
            server.client_url,
            tls=ssl_context,
            tls_handshake_first=True,
            timeout=2.0,
        )

        # Verify we can publish and subscribe
        await client.publish("test.subject", b"Hello TLS")
        await client.flush()

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_tls_handshake_first_with_hostname_verification():
    """Test TLS connection with hostname verification."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_tls_handshake_first.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Create SSL context with custom CA (using self-signed cert as CA)
        ssl_context = ssl.create_default_context(
            cafile=os.path.join(os.path.dirname(__file__), "certs", "server-cert.pem")
        )

        # Connect with TLS and hostname verification
        client = await connect(
            server.client_url,
            tls=ssl_context,
            tls_hostname="localhost",
            tls_handshake_first=True,
            timeout=2.0,
        )

        # Verify connection works
        await client.publish("test", b"data")
        await client.flush()

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_tls_without_handshake_first():
    """Test TLS connection without handshake first (normal TLS mode)."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_tls_upgrade.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Create SSL context that trusts our self-signed certificate
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Connect with TLS but without handshake first
        # Server has TLS configured, so it will advertise tls_available/tls_required in INFO
        # and we'll upgrade after receiving INFO
        client = await connect(
            server.client_url,
            tls=ssl_context,
            tls_handshake_first=False,
            timeout=2.0,
        )

        # Verify we can publish over TLS connection
        await client.publish("test.tls", b"TLS without handshake first")
        await client.flush()

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_tls_reconnection_preserves_settings():
    """Test that TLS settings are preserved across reconnections."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_tls_handshake_first.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)
    server_port = server.port

    try:
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Use event to wait for reconnection
        reconnected = asyncio.Event()

        def on_reconnected():
            reconnected.set()

        client = await connect(
            server.client_url,
            tls=ssl_context,
            tls_handshake_first=True,
            timeout=2.0,
        )

        client.add_reconnected_callback(on_reconnected)

        # Publish a message to verify connection
        await client.publish("test.before", b"Before reconnect")
        await client.flush()

        # Shutdown server to trigger reconnection
        await server.shutdown()

        # Start new server on same port (small delay for port to be released)
        await asyncio.sleep(0.1)
        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)

        # Wait for reconnection event (with timeout)
        await asyncio.wait_for(reconnected.wait(), timeout=5.0)

        # Verify we can still publish over TLS after reconnection
        await client.publish("test.after", b"After reconnect")
        await client.flush()

        await client.close()
        await new_server.shutdown()
    except Exception:
        await server.shutdown()
        raise


@pytest.mark.asyncio
async def test_tls_reconnection_with_upgrade_mode():
    """Reconnect must preserve TLS upgrade-mode handshake (no handshake_first).

    With ``tls_handshake_first=False`` the initial connect reads a plaintext
    INFO, then upgrades to TLS when ``tls_required`` is set. The reconnect
    path must follow the same sequence; otherwise it either bypasses the
    upgrade (plaintext CONNECT with credentials) or tries a TLS-first
    handshake against an upgrade-mode server.
    """
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_tls_upgrade.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)
    server_port = server.port

    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        reconnected = asyncio.Event()

        client = await connect(
            server.client_url,
            tls=ssl_context,
            tls_handshake_first=False,
            allow_reconnect=True,
            reconnect_time_wait=0.1,
            timeout=2.0,
        )
        client.add_reconnected_callback(lambda: reconnected.set())

        await server.shutdown()
        await asyncio.sleep(0.1)

        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)
        try:
            await asyncio.wait_for(reconnected.wait(), timeout=5.0)

            # Verify the reconnected connection is usable end-to-end.
            subscription = await client.subscribe("test.reconnect.upgrade")
            await client.publish("test.reconnect.upgrade", b"after upgrade-mode reconnect")
            await client.flush()
            message = await asyncio.wait_for(subscription.next(), timeout=2.0)
            assert message.data == b"after upgrade-mode reconnect"
        finally:
            await new_server.shutdown()
            await client.close()
    finally:
        try:
            await server.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_tls_reconnect_from_plain_to_tls_required():
    """A reconnect that finds the server now requiring TLS must upgrade before sending CONNECT.

    The initial server is plaintext; the replacement server requires TLS via the
    upgrade-mode flow. The reconnect path must perform the TLS upgrade against
    the freshly-received INFO, otherwise it would either send the plaintext
    CONNECT (leaking credentials) or fail to handshake at all.
    """
    plain_server = await run(port=0, timeout=5.0)
    server_port = plain_server.port

    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        reconnected = asyncio.Event()

        client = await connect(
            plain_server.client_url,
            tls=ssl_context,
            allow_reconnect=True,
            reconnect_time_wait=0.1,
            timeout=2.0,
        )
        client.add_reconnected_callback(lambda: reconnected.set())

        await plain_server.shutdown()
        await asyncio.sleep(0.1)

        tls_config = os.path.join(os.path.dirname(__file__), "configs", "server_tls_upgrade.conf")
        tls_server = await run(config_path=tls_config, port=server_port, timeout=5.0)
        try:
            await asyncio.wait_for(reconnected.wait(), timeout=5.0)

            assert client.server_info is not None
            assert client.server_info.tls_required is True

            subscription = await client.subscribe("test.reconnect.tls.upgrade")
            await client.publish("test.reconnect.tls.upgrade", b"after tls-required reconnect")
            await client.flush()
            message = await asyncio.wait_for(subscription.next(), timeout=2.0)
            assert message.data == b"after tls-required reconnect"
        finally:
            await tls_server.shutdown()
            await client.close()
    finally:
        with contextlib.suppress(Exception):
            await plain_server.shutdown()


@pytest.mark.asyncio
async def test_tls_verify_with_client_certificate():
    """Test TLS connection with client certificate verification."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_tls_verify.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Create SSL context with client certificate
        ssl_context = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH, cafile=os.path.join(os.path.dirname(__file__), "certs", "ca.pem")
        )
        # Load client certificate and key
        ssl_context.load_cert_chain(
            certfile=os.path.join(os.path.dirname(__file__), "certs", "client-cert.pem"),
            keyfile=os.path.join(os.path.dirname(__file__), "certs", "client-key.pem"),
        )

        # Connect with TLS and client certificate
        client = await connect(
            server.client_url,
            tls=ssl_context,
            tls_hostname="localhost",
            tls_handshake_first=True,
            timeout=2.0,
        )

        # Verify we can publish
        await client.publish("test.tls.verify", b"TLS with client verification works!")
        await client.flush()

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_tls_scheme_against_plaintext_server_raises(server: Server):
    """tls:// URL must fail fast when the server offers no TLS."""
    plaintext_url = server.client_url.replace("nats://", "tls://")
    with pytest.raises(SecureConnectionRequiredError):
        await connect(plaintext_url, timeout=1.0, allow_reconnect=False)


@pytest.mark.asyncio
async def test_tls_context_against_plaintext_server_raises(server: Server):
    """Passing a tls context to a plaintext server must fail fast."""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    with pytest.raises(SecureConnectionRequiredError):
        await connect(server.client_url, tls=ssl_context, timeout=1.0, allow_reconnect=False)


@pytest.mark.asyncio
async def test_tls_connection_without_ssl_context_fails():
    """Test that connecting to TLS server without SSL context fails."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_tls_handshake_first.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Try to connect without SSL context to a TLS server
        # This should fail because the server expects TLS
        # Use short timeout since we expect immediate failure
        with pytest.raises(Exception):
            await connect(
                server.client_url,
                timeout=0.5,
                allow_reconnect=False,
            )
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_tls_handshake_first_against_server_without_tls_advertise():
    """A TLS-terminator that doesn't advertise ``tls_available`` must not raise after a successful handshake-first connect."""
    certs = Path(__file__).parent / "certs"
    server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    server_ctx.load_cert_chain(certfile=str(certs / "server-cert.pem"), keyfile=str(certs / "server-key.pem"))

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # INFO omits both tls_required and tls_available, mirroring a TLS terminator
        # that fronts a plaintext NATS server.
        info = (
            b'INFO {"server_id":"test","server_name":"test","version":"2.0.0","proto":1,'
            b'"go":"go1.20","host":"127.0.0.1","port":4222,"headers":true,"max_payload":1048576}\r\n'
        )
        writer.write(info)
        await writer.drain()
        await reader.readline()  # CONNECT
        await reader.readline()  # PING
        writer.write(b"PONG\r\n")
        await writer.drain()
        while await reader.read(4096):
            pass
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    listener = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_ctx)
    host, port = listener.sockets[0].getsockname()[:2]

    client_ctx = ssl.create_default_context()
    client_ctx.check_hostname = False
    client_ctx.verify_mode = ssl.CERT_NONE

    try:
        client = await connect(
            f"nats://{host}:{port}",
            tls=client_ctx,
            tls_handshake_first=True,
            timeout=2.0,
            allow_reconnect=False,
        )
        try:
            assert client.status == ClientStatus.CONNECTED
        finally:
            await client.close()
    finally:
        listener.close()
        await listener.wait_closed()
