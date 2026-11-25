"""One deadline spans transport setup, TLS, CONNECT and PONG."""

import asyncio
from typing import cast
from unittest.mock import AsyncMock, Mock

import pytest
from nats.client import ClientStatus, connect
from nats.client.connection import Connection, TcpConnection, complete_handshake, establish_connection
from nats.client.protocol.types import ConnectInfo


@pytest.mark.parametrize("frame", [b"+OK\r\n", b"PING\r\n", b"-ERR 'Invalid Subject'\r\n"])
async def test_handshake_traffic_cannot_extend_deadline(frame):
    class Stream:
        write = AsyncMock()

        async def readline(self):
            await asyncio.sleep(0.01)
            return frame

    async with asyncio.timeout(1):
        with pytest.raises(TimeoutError):
            await complete_handshake(
                cast(Connection, Stream()),
                ConnectInfo(lang="python", version="test", verbose=False, pedantic=False, tls_required=False),
                timeout=0.05,
            )


async def test_transport_and_pong_share_remaining_budget(monkeypatch):
    import nats.client.connection as module

    class Stream:
        write = AsyncMock()
        close = AsyncMock()
        count = 0

        async def readline(self):
            self.count += 1
            if self.count == 1:
                return b"INFO {}\r\n"
            await asyncio.sleep(0.2)
            return b"PONG\r\n"

    stream = Stream()

    async def open_connection(*args, **kwargs):
        await asyncio.sleep(0.06)
        return stream

    monkeypatch.setattr(module, "open_tcp_connection", open_connection)
    deadline = asyncio.get_running_loop().time() + 0.1
    connection, _, _ = await establish_connection("nats://host:4222", timeout=0.1, deadline=deadline, wants_tls=False)
    with pytest.raises(TimeoutError):
        await complete_handshake(
            connection,
            ConnectInfo(lang="python", version="test", verbose=False, pedantic=False, tls_required=False),
            timeout=0.1,
            deadline=deadline,
        )


async def test_tls_upgrade_is_bounded(monkeypatch):
    import nats.client.connection as module

    reader = asyncio.StreamReader()
    reader.feed_data(b'INFO {"tls_required":true}\r\n')
    connection = TcpConnection(reader, Mock())

    async def stalled(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(connection, "upgrade_to_tls", stalled)
    close = AsyncMock()
    monkeypatch.setattr(connection, "close", close)
    monkeypatch.setattr(module, "open_tcp_connection", AsyncMock(return_value=connection))
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(establish_connection("nats://host:4222", timeout=0.03, wants_tls=False), 0.5)
    close.assert_awaited_once()


async def test_close_aborts_stalled_tls_shutdown(monkeypatch):
    import nats.client.connection as module

    monkeypatch.setattr(module, "ABANDONED_CLOSE_TIMEOUT", 0.01)
    writer = Mock()
    writer.wait_closed = AsyncMock(side_effect=lambda: None)

    async def stalled():
        await asyncio.Event().wait()

    writer.wait_closed.side_effect = stalled
    connection = TcpConnection(asyncio.StreamReader(), writer)
    with pytest.raises(TimeoutError):
        await connection.close()
    writer.transport.abort.assert_called_once()
    assert not connection.is_connected()


async def test_first_reconnect_does_not_wait_for_backoff(server):
    client = await connect(server.client_url, reconnect_time_wait=30)
    try:
        await asyncio.wait_for(client.force_reconnect(), 1)
        assert client.status == ClientStatus.CONNECTED
    finally:
        await client.close()


async def test_clean_close_does_not_abort_transport():
    writer = Mock()
    writer.wait_closed = AsyncMock()
    connection = TcpConnection(asyncio.StreamReader(), writer)
    await connection.close()
    writer.wait_closed.assert_awaited_once()
    writer.transport.abort.assert_not_called()


async def test_cancelled_close_aborts_transport():
    entered = asyncio.Event()

    async def stalled():
        entered.set()
        await asyncio.Event().wait()

    writer = Mock()
    writer.wait_closed = AsyncMock(side_effect=stalled)
    connection = TcpConnection(asyncio.StreamReader(), writer)
    task = asyncio.create_task(connection.close())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    writer.transport.abort.assert_called_once()
    assert not connection.is_connected()
