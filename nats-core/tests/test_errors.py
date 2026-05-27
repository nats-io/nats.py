"""Tests for typed server error mapping."""

import asyncio

import pytest
from nats.client import (
    AuthenticationExpiredError,
    AuthorizationViolationError,
    InvalidSubjectError,
    MaxConnectionsExceededError,
    MaxPayloadServerError,
    ParserViolationError,
    PermissionsViolationError,
    SecureConnectionRequiredError,
    ServerError,
    StaleConnectionError,
    connect,
    server_error_from_message,
)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("Authorization Violation", AuthorizationViolationError),
        ("'Authorization Violation'", AuthorizationViolationError),
        ("authorization violation for user X", AuthorizationViolationError),
        ("Authentication Timeout", AuthorizationViolationError),
        ("Authentication Expired", AuthenticationExpiredError),
        ("Permissions Violation for Publish to foo.bar", PermissionsViolationError),
        ("Permissions Violation for Subscription to foo.>", PermissionsViolationError),
        ("Stale Connection", StaleConnectionError),
        ("Maximum Connections Exceeded", MaxConnectionsExceededError),
        ("Maximum Payload Violation", MaxPayloadServerError),
        ("Invalid Subject", InvalidSubjectError),
        ("Parser Error", ParserViolationError),
        ("Secure Connection - TLS Required", SecureConnectionRequiredError),
    ],
)
def test_server_error_from_message_maps_known_categories(message, expected):
    """Each known server -ERR variant maps to its typed subclass."""
    err = server_error_from_message(message)
    assert isinstance(err, expected)
    # The raw text is preserved verbatim for downstream inspection.
    assert err.message == message
    assert str(err) == message


def test_server_error_from_message_unknown_falls_back_to_base():
    """Unrecognized -ERR text yields a bare ServerError that still carries the raw message."""
    err = server_error_from_message("Some Future Error Category We Have Not Mapped")
    assert type(err) is ServerError
    assert err.message == "Some Future Error Category We Have Not Mapped"


def test_server_error_is_exception():
    """ServerError must remain a plain Exception subclass for `except Exception` paths."""
    assert issubclass(ServerError, Exception)
    assert issubclass(AuthorizationViolationError, ServerError)


async def _read_until(reader: asyncio.StreamReader, needle: bytes) -> bytes:
    """Read from ``reader`` until ``needle`` appears, then return everything consumed."""
    buf = b""
    while needle not in buf:
        chunk = await reader.read(4096)
        if not chunk:
            break
        buf += chunk
    return buf


@pytest.mark.asyncio
async def test_initial_connect_raises_typed_error_on_auth_violation():
    """A `-ERR 'Authorization Violation'` during the connect handshake surfaces as the typed exception."""

    async def mock_server(reader, writer):
        info = (
            b'INFO {"server_id":"test","version":"2.0.0","go":"go1.20","host":"127.0.0.1",'
            b'"port":4222,"headers":true,"max_payload":1048576}\r\n'
        )
        writer.write(info)
        await writer.drain()

        # Wait until the client has sent its CONNECT + PING before rejecting,
        # otherwise the client may read the -ERR before its own write completes.
        await _read_until(reader, b"PING\r\n")

        writer.write(b"-ERR 'Authorization Violation'\r\n")
        await writer.drain()
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    server = await asyncio.start_server(mock_server, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    url = f"nats://{host}:{port}"

    try:
        with pytest.raises(AuthorizationViolationError) as exc_info:
            await connect(url, timeout=2.0, allow_reconnect=False)
        assert "authorization" in exc_info.value.message.lower()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_error_callback_receives_typed_stale_connection_error():
    """A `-ERR 'Stale Connection'` sent on an established connection reaches the callback as the typed exception."""

    handshake_done = asyncio.Event()

    async def mock_server(reader, writer):
        info = (
            b'INFO {"server_id":"test","version":"2.0.0","go":"go1.20","host":"127.0.0.1",'
            b'"port":4222,"headers":true,"max_payload":1048576}\r\n'
        )
        writer.write(info)
        await writer.drain()

        # CONNECT + PING from the client; ack with PONG so connect() returns.
        await _read_until(reader, b"PING\r\n")
        writer.write(b"PONG\r\n")
        await writer.drain()
        handshake_done.set()

        # Send the typed -ERR after the client is connected, then hold the socket
        # open briefly so the read loop processes the frame before EOF triggers
        # the disconnect path.
        await asyncio.sleep(0.05)
        writer.write(b"-ERR 'Stale Connection'\r\n")
        await writer.drain()
        await asyncio.sleep(0.2)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    server = await asyncio.start_server(mock_server, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    url = f"nats://{host}:{port}"

    try:
        client = await connect(url, timeout=2.0, allow_reconnect=False)
        errors: list[Exception] = []
        client.add_error_callback(errors.append)

        await asyncio.wait_for(handshake_done.wait(), timeout=2.0)

        # Wait for the -ERR to be observed.
        for _ in range(50):
            if errors:
                break
            await asyncio.sleep(0.02)

        assert errors, "error callback was never invoked"
        assert isinstance(errors[0], StaleConnectionError)
        assert errors[0].message == "Stale Connection"
        assert isinstance(client.last_error, StaleConnectionError)

        await client.close()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_pending_request_fails_with_typed_error():
    """An in-flight request awaiting a reply fails with the typed -ERR instead of timing out."""

    handshake_done = asyncio.Event()
    saw_publish = asyncio.Event()

    async def mock_server(reader, writer):
        info = (
            b'INFO {"server_id":"test","version":"2.0.0","go":"go1.20","host":"127.0.0.1",'
            b'"port":4222,"headers":true,"max_payload":1048576}\r\n'
        )
        writer.write(info)
        await writer.drain()

        await _read_until(reader, b"PING\r\n")
        writer.write(b"PONG\r\n")
        await writer.drain()
        handshake_done.set()

        # Drain incoming lines until we see a PUB from the client, then send the -ERR.
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                if line.startswith(b"PUB ") or line.startswith(b"HPUB "):
                    saw_publish.set()
                    writer.write(b"-ERR 'Authorization Violation'\r\n")
                    await writer.drain()
                    break
        except Exception:
            pass
        # Hold the socket open briefly so the client processes the -ERR.
        await asyncio.sleep(0.2)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    server = await asyncio.start_server(mock_server, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    url = f"nats://{host}:{port}"

    try:
        client = await connect(url, timeout=2.0, allow_reconnect=False)
        await asyncio.wait_for(handshake_done.wait(), timeout=2.0)

        with pytest.raises(AuthorizationViolationError):
            # Generous timeout — if the typed error did not propagate, the test
            # would otherwise raise TimeoutError after this elapses.
            await client.request("svc.test", b"hello", timeout=2.0)

        assert saw_publish.is_set()
        await client.close()
    finally:
        server.close()
        await server.wait_closed()
