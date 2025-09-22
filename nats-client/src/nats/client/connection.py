"""Connection classes for NATS client."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ssl

logger = logging.getLogger("nats.client")


class Connection(ABC):
    """Abstract base class for NATS connections."""

    @abstractmethod
    async def close(self) -> None:
        """Close the connection."""

    @abstractmethod
    async def read(self, n: int) -> bytes:
        """Read n bytes from the connection."""

    @abstractmethod
    async def write(self, data: bytes) -> None:
        """Write data to the connection."""

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the connection is active."""

    async def readline(self) -> bytes:
        """Read a line from the connection.

        Returns:
            Line read from the connection ending with newline
        """
        raise NotImplementedError

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes from the connection.

        Args:
            n: Number of bytes to read

        Returns:
            Bytes read

        Raises:
            asyncio.IncompleteReadError: If fewer than n bytes are available
        """
        raise NotImplementedError


class TcpConnection(Connection):
    """TCP-based NATS connection."""

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
        host: str, port: int
    ):
        """Initialize TCP connection.

        Args:
            reader: Stream reader for the connection
            writer: Stream writer for the connection
            host: Server hostname or IP address
            port: Server port number
        """
        self.host = host
        self.port = port
        self._reader = reader
        self._writer = writer

    async def close(self) -> None:
        """Close TCP connection."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None
            logger.debug(
                "TCP connection closed to %s:%s", self.host, self.port
            )

    async def read(self, n: int) -> bytes:
        """Read n bytes from TCP connection."""
        if not self._reader:
            msg = "Not connected"
            raise ConnectionError(msg)
        return await self._reader.read(n)

    async def write(self, data: bytes) -> None:
        """Write data to TCP connection."""
        if not self._writer:
            msg = "Not connected"
            raise ConnectionError(msg)
        self._writer.write(data)
        await self._writer.drain()

    def is_connected(self) -> bool:
        """Check if TCP connection is active."""
        return self._writer is not None and not self._writer.is_closing()

    async def readline(self) -> bytes:
        """Read a line from TCP connection.

        Returns:
            A line of bytes ending with newline

        Raises:
            ConnectionError: If not connected
        """
        if not self._reader:
            msg = "Not connected"
            raise ConnectionError(msg)
        return await self._reader.readline()

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes from TCP connection.

        Args:
            n: Number of bytes to read

        Returns:
            Exactly n bytes

        Raises:
            ConnectionError: If not connected
            asyncio.IncompleteReadError: If connection closed before n bytes were read
        """
        if not self._reader:
            msg = "Not connected"
            raise ConnectionError(msg)
        return await self._reader.readexactly(n)


async def open_tcp_connection(
    host: str,
    port: int,
    ssl_context: ssl.SSLContext | None = None
) -> TcpConnection:
    """Open a TCP connection to a NATS server.

    Args:
        host: Server hostname
        port: Server port
        ssl_context: Optional SSL context for TLS

    Returns:
        TCP connection

    Raises:
        ConnectionError: If connection fails
    """
    try:
        reader, writer = await asyncio.open_connection(
            host, port, ssl=ssl_context
        )
        return TcpConnection(reader, writer, host, port)
    except Exception as e:
        msg = f"Failed to connect: {e}"
        raise ConnectionError(msg)
