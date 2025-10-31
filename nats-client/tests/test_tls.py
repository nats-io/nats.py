"""Tests for TLS functionality in NATS client."""

import os
import ssl

import pytest
from nats.client import connect
from nats.server import run


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

        client = await connect(
            server.client_url,
            tls=ssl_context,
            tls_handshake_first=True,
            timeout=2.0,
        )

        # Publish a message to verify connection
        await client.publish("test.before", b"Before reconnect")
        await client.flush()

        # Shutdown server to trigger reconnection
        await server.shutdown()

        # Start new server on same port
        import asyncio

        await asyncio.sleep(0.5)
        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)

        # Wait for reconnection
        await asyncio.sleep(3.0)

        # Verify we can still publish over TLS after reconnection
        await client.publish("test.after", b"After reconnect")
        await client.flush()

        await client.close()
        await new_server.shutdown()
    except Exception:
        await server.shutdown()
        raise


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
async def test_tls_connection_without_ssl_context_fails():
    """Test that connecting to TLS server without SSL context fails."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_tls_handshake_first.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Try to connect without SSL context to a TLS server
        # This should fail because the server expects TLS
        with pytest.raises(Exception):  # Could be ConnectionError or timeout
            await connect(
                server.client_url,
                timeout=2.0,
            )
    finally:
        await server.shutdown()
