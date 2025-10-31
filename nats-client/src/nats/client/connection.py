"""Connection classes for NATS client."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import ssl

logger = logging.getLogger("nats.client")


@runtime_checkable
class Connection(Protocol):
    """Protocol for NATS connections.

    This is a structural type (Protocol) rather than a nominal type (ABC),
    allowing any class with the required methods to be used as a connection
    without explicit inheritance.
    """

    async def close(self) -> None:
        """Close the connection."""
        ...

    async def read(self, n: int) -> bytes:
        """Read n bytes from the connection."""
        ...

    async def write(self, data: bytes) -> None:
        """Write data to the connection."""
        ...

    def is_connected(self) -> bool:
        """Check if the connection is active."""
        ...

    async def readline(self) -> bytes:
        """Read a line from the connection.

        Returns:
            Line read from the connection ending with newline
        """
        ...

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes from the connection.

        Args:
            n: Number of bytes to read

        Returns:
            Bytes read

        Raises:
            asyncio.IncompleteReadError: If fewer than n bytes are available
        """
        ...


class TcpConnection:
    """TCP-based NATS connection.

    Implements the Connection protocol for TCP connections.
    """

    _reader: asyncio.StreamReader | None
    _writer: asyncio.StreamWriter | None

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Initialize TCP connection.

        Args:
            reader: Stream reader for the connection
            writer: Stream writer for the connection
        """
        self._reader = reader
        self._writer = writer

    async def upgrade_to_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
    ) -> None:
        """Upgrade existing connection to TLS.

        Args:
            ssl_context: SSL context for TLS
            server_hostname: Hostname for SSL certificate verification

        Raises:
            ConnectionError: If upgrade fails
        """
        if not self._writer:
            msg = "Not connected"
            raise ConnectionError(msg)

        try:
            # Get the transport and protocol from the writer
            transport = self._writer.transport
            protocol = transport.get_protocol()

            # Start TLS upgrade on the transport
            loop = asyncio.get_running_loop()
            new_transport = await loop.start_tls(
                transport,
                protocol,
                ssl_context,
                server_hostname=server_hostname,
            )

            # Update the writer's transport
            self._writer._transport = new_transport  # type: ignore[attr-defined]
            logger.debug("Connection upgraded to TLS")

        except Exception as e:
            msg = f"Failed to upgrade connection to TLS: {e}"
            raise ConnectionError(msg) from e

    async def close(self) -> None:
        """Close TCP connection."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None
            logger.debug("TCP connection closed")

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
    ssl_context: ssl.SSLContext | None = None,
    server_hostname: str | None = None,
) -> TcpConnection:
    """Open a TCP connection to a NATS server.

    Args:
        host: Server hostname
        port: Server port
        ssl_context: Optional SSL context for TLS
        server_hostname: Hostname for SSL certificate verification (defaults to host)

    Returns:
        TCP connection

    Raises:
        ConnectionError: If connection fails
    """
    try:
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl_context, server_hostname=server_hostname)
        return TcpConnection(reader, writer)
    except Exception as e:
        msg = f"Failed to connect: {e}"
        raise ConnectionError(msg)
