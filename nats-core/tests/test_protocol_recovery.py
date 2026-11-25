"""Connection continuity and handshake protocol regressions."""

import asyncio
import json
from typing import cast
from unittest.mock import AsyncMock, Mock

import pytest
from nats.client import ClientStatus
from nats.client.connection import Connection, TcpConnection, complete_handshake
from nats.client.errors import AuthorizationViolationError, PermissionsViolationError
from nats.client.protocol.message import Info, MalformedHeadersError, parse
from nats.client.protocol.types import ConnectInfo


class Stream:
    def __init__(self, data):
        self.reader = asyncio.StreamReader()
        self.reader.feed_data(data)
        self.reader.feed_eof()
        self.write = AsyncMock()

    async def readline(self):
        return await self.reader.readline()

    async def readexactly(self, n):
        return await self.reader.readexactly(n)


async def test_handshake_services_ping_and_nonfatal_error():
    stream = Stream(b"+OK\r\nPING\r\n-ERR 'Permissions Violation for Publish to x'\r\nPONG\r\n")
    errors = await complete_handshake(
        cast(Connection, stream),
        ConnectInfo(lang="python", version="test", verbose=False, pedantic=False, tls_required=False),
        timeout=1,
    )
    assert isinstance(errors[0], PermissionsViolationError)
    assert stream.write.call_args_list[-1].args == (b"PONG\r\n",)


async def test_handshake_preserves_fatal_error():
    with pytest.raises(AuthorizationViolationError):
        await complete_handshake(
            cast(Connection, Stream(b"-ERR 'Authorization Violation'\r\n")),
            ConnectInfo(lang="python", version="test", verbose=False, pedantic=False, tls_required=False),
            timeout=1,
        )


async def test_invalid_headers_consume_only_their_message():
    stream = Stream(b"HMSG x 1 5 8\r\nbad\r\nabc\r\nPING\r\n")
    with pytest.raises(MalformedHeadersError):
        await parse(stream)
    response = await parse(stream)
    assert response is not None and response.op == "PING"


async def test_large_info_exceeds_tcp_reader_limit():
    data = json.dumps({"connect_urls": [f"node{i}:4222" for i in range(10000)]}).encode()
    reader = asyncio.StreamReader(limit=4096)
    reader.feed_data(b"INFO " + data + b"\r\nPING\r\n")
    reader.feed_eof()
    connection = TcpConnection(reader, Mock())
    info = await parse(connection)
    assert isinstance(info, Info)
    assert len(info.info["connect_urls"]) == 10000
    response = await parse(connection)
    assert response is not None and response.op == "PING"


@pytest.mark.parametrize("data", [b"INFO {broken}\r\n", b"INFO {}\r\n", b"HMSG x 1 5 8\r\nbad\r\nabc\r\n"])
async def test_bad_info_or_headers_leave_established_connection_up(client, monkeypatch, data):
    reported = asyncio.Event()
    client.add_error_callback(lambda error: reported.set())
    connection = client._connection
    original = connection.readline
    stream = Stream(data)
    lines = True

    async def read_line():
        nonlocal lines
        if lines:
            lines = False
            return await stream.readline()
        return await original()

    if data.startswith(b"HMSG"):
        monkeypatch.setattr(connection, "readexactly", stream.readexactly)
    monkeypatch.setattr(connection, "readline", read_line)
    # Wake the existing read, then let it consume the injected line.
    await client.flush(timeout=1)
    await asyncio.wait_for(reported.wait(), 1)
    assert client.status == ClientStatus.CONNECTED
    await client.flush(timeout=1)


@pytest.mark.parametrize("transport", ["tcp", "websocket"])
@pytest.mark.parametrize("ending", [b"\r\n", b""])
async def test_info_line_has_a_hard_memory_bound(monkeypatch, transport, ending):
    import nats.client.connection as module

    monkeypatch.setattr(module, "MAX_CONTROL_LINE_SIZE", 1024, raising=False)
    data = b"INFO " + b"x" * 1024 + ending
    if transport == "tcp":
        reader = asyncio.StreamReader(limit=64)
        reader.feed_data(data)
        reader.feed_eof()
        connection = module.TcpConnection(reader, Mock())
    else:
        ws = Mock()
        ws.recv = AsyncMock(side_effect=[data[:512], data[512:]])
        connection = module.WebSocketConnection(ws)
    with pytest.raises(ConnectionError, match="Control line too long"):
        await connection.readline()


async def test_websocket_line_limit_does_not_count_following_messages(monkeypatch):
    import nats.client.connection as module

    monkeypatch.setattr(module, "MAX_CONTROL_LINE_SIZE", 32, raising=False)
    ws = Mock()
    ws.recv = AsyncMock(return_value=b"PING\r\n" * 100)
    connection = module.WebSocketConnection(ws)
    for _ in range(100):
        assert await connection.readline() == b"PING\r\n"
