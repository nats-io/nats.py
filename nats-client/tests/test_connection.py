"""Tests for connection module."""

import asyncio

import pytest
from nats.client.connection import TcpConnection


async def create_echo_server_connection() -> tuple[TcpConnection, asyncio.Server]:
    """Create a real connection using a local echo server.

    Returns:
        Tuple of (TcpConnection, server)
    """

    async def handle_client(reader, writer):
        """Echo server handler."""
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    connection = TcpConnection(reader, writer)

    return connection, server


async def test_tcp_connection_read_when_connected():
    """Test reading from a connected TCP connection."""
    connection, server = await create_echo_server_connection()

    try:
        # Write some data to ourselves (echo server will return it)
        await connection.write(b"test data")
        result = await connection.read(9)
        assert result == b"test data"
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_tcp_connection_read_when_not_connected():
    """Test reading from a disconnected TCP connection raises error."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.close()

        with pytest.raises(ConnectionError, match="Not connected"):
            await connection.read(100)
    finally:
        server.close()
        await server.wait_closed()


async def test_tcp_connection_write_when_connected():
    """Test writing to a connected TCP connection."""
    connection, server = await create_echo_server_connection()

    try:
        # Should not raise
        await connection.write(b"test data")
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_tcp_connection_write_when_not_connected():
    """Test writing to a disconnected TCP connection raises error."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.close()

        with pytest.raises(ConnectionError, match="Not connected"):
            await connection.write(b"test data")
    finally:
        server.close()
        await server.wait_closed()


async def test_tcp_connection_readline_when_connected():
    """Test reading a line from a connected TCP connection."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.write(b"test line\n")
        result = await connection.readline()
        assert result == b"test line\n"
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_tcp_connection_readline_when_not_connected():
    """Test reading a line from a disconnected TCP connection raises error."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.close()

        with pytest.raises(ConnectionError, match="Not connected"):
            await connection.readline()
    finally:
        server.close()
        await server.wait_closed()


async def test_tcp_connection_readexactly_when_connected():
    """Test reading exactly n bytes from a connected TCP connection."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.write(b"exactly5")
        result = await connection.readexactly(8)
        assert result == b"exactly5"
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_tcp_connection_readexactly_when_not_connected():
    """Test reading exactly n bytes from a disconnected TCP connection raises error."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.close()

        with pytest.raises(ConnectionError, match="Not connected"):
            await connection.readexactly(5)
    finally:
        server.close()
        await server.wait_closed()


async def test_tcp_connection_is_connected_when_connected():
    """Test is_connected returns True for an active TCP connection."""
    connection, server = await create_echo_server_connection()

    try:
        assert connection.is_connected() is True
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_tcp_connection_is_connected_after_close():
    """Test is_connected returns False after closing TCP connection."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.close()
        assert connection.is_connected() is False
    finally:
        server.close()
        await server.wait_closed()


async def test_tcp_connection_close_sets_reader_and_writer_to_none():
    """Test that close properly cleans up TCP connection reader and writer."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.close()
        assert connection._reader is None
        assert connection._writer is None
    finally:
        server.close()
        await server.wait_closed()


async def test_tcp_connection_close_when_already_closed():
    """Test that closing an already closed TCP connection is safe."""
    connection, server = await create_echo_server_connection()

    try:
        await connection.close()

        # Close again - should not raise error
        await connection.close()

        assert connection._reader is None
        assert connection._writer is None
    finally:
        server.close()
        await server.wait_closed()
