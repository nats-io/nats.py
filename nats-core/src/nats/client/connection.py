"""Connection classes for NATS client."""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from urllib.parse import urlparse

from nats.client.errors import SecureConnectionRequiredError
from nats.client.protocol.message import Info, parse
from nats.client.protocol.types import ServerInfo as ProtocolServerInfo

if TYPE_CHECKING:
    from websockets.asyncio.client import ClientConnection

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
            transport = self._writer.transport
            protocol = transport.get_protocol()

            loop = asyncio.get_running_loop()
            new_transport = await loop.start_tls(
                transport,
                protocol,
                ssl_context,
                server_hostname=server_hostname,
            )

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


class WebSocketConnection:
    """WebSocket-based NATS connection.

    Adapts the message-framed WebSocket transport into the byte-stream
    Connection protocol expected by the NATS parser. Incoming frames are
    buffered so that readline/readexactly/read can be served at byte
    granularity regardless of frame boundaries.
    """

    _ws: ClientConnection | None
    _buffer: bytes

    def __init__(self, ws: ClientConnection) -> None:
        # ``websockets`` is guaranteed importable here — the caller already
        # imported ``connect`` from it to obtain ``ws``.
        from websockets.protocol import State

        self._ws = ws
        self._buffer = b""
        self._open_state = State.OPEN

    async def _fill_buffer(self) -> None:
        if self._ws is None:
            msg = "Not connected"
            raise ConnectionError(msg)
        try:
            frame = await self._ws.recv()
        except Exception as e:
            raise asyncio.IncompleteReadError(self._buffer, None) from e
        if isinstance(frame, str):
            frame = frame.encode()
        if not frame:
            raise asyncio.IncompleteReadError(self._buffer, None)
        self._buffer += frame

    async def close(self) -> None:
        """Close WebSocket connection."""
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
            logger.debug("WebSocket connection closed")

    async def read(self, n: int) -> bytes:
        """Read up to n bytes from the WebSocket connection."""
        if not self._buffer:
            try:
                await self._fill_buffer()
            except asyncio.IncompleteReadError:
                return b""
        result = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return result

    async def write(self, data: bytes) -> None:
        """Write data to the WebSocket connection as a binary frame."""
        if self._ws is None:
            msg = "Not connected"
            raise ConnectionError(msg)
        await self._ws.send(data)

    def is_connected(self) -> bool:
        """Check if WebSocket connection is active."""
        if self._ws is None:
            return False
        return self._ws.state is self._open_state

    async def readline(self) -> bytes:
        """Read a line (ending in CRLF) from the WebSocket connection."""
        while b"\n" not in self._buffer:
            await self._fill_buffer()
        idx = self._buffer.index(b"\n") + 1
        result = self._buffer[:idx]
        self._buffer = self._buffer[idx:]
        return result

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes from the WebSocket connection."""
        while len(self._buffer) < n:
            await self._fill_buffer()
        result = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return result


async def open_websocket_connection(
    url: str,
    ssl_context: ssl.SSLContext | None = None,
    server_hostname: str | None = None,
) -> WebSocketConnection:
    """Open a WebSocket connection to a NATS server.

    Args:
        url: Full ws:// or wss:// URL
        ssl_context: Optional SSL context for wss://
        server_hostname: Hostname for SSL certificate verification

    Returns:
        WebSocket connection

    Raises:
        ConnectionError: If connection fails
        ImportError: If the websockets package is not installed
    """
    try:
        from websockets.asyncio.client import connect as ws_connect
    except ImportError as e:
        msg = "WebSocket transport requires the 'websockets' package. Install nats-core[websocket]."
        raise ImportError(msg) from e

    try:
        ws = await ws_connect(
            url,
            ssl=ssl_context,
            server_hostname=server_hostname,
            max_size=None,
        )
        return WebSocketConnection(ws)
    except Exception as e:
        msg = f"Failed to connect: {e}"
        raise ConnectionError(msg) from e


async def establish_connection(
    url: str,
    *,
    timeout: float,
    wants_tls: bool,
    tls: ssl.SSLContext | None = None,
    tls_hostname: str | None = None,
    tls_handshake_first: bool = False,
) -> tuple[Connection, ProtocolServerInfo, bool]:
    """Open a transport, read the server INFO, and finalize TLS.

    The returned connection has completed the INFO exchange and any TLS
    upgrade implied by the client's intent and the server's advertisement.
    The caller is responsible for sending CONNECT and continuing the
    protocol setup.

    Args:
        url: Full server URL (nats://, tls://, ws://, wss://).
        timeout: Hard timeout applied independently to the transport open and
            the INFO read.
        wants_tls: Client demanded TLS (via scheme, an explicit ``tls=``
            context, or ``tls_handshake_first``). Drives the
            SecureConnectionRequired guard and the opt-in upgrade when the
            server advertises ``tls_required`` or ``tls_available``.
        tls: SSL context to use for the handshake-first connect and for any
            post-INFO upgrade. Defaults to ``ssl.create_default_context()``
            when ``wants_tls`` is set and ``tls`` is None.
        tls_hostname: Override hostname for cert verification. Defaults to
            the URL host.
        tls_handshake_first: Perform the TLS handshake immediately on TCP,
            before reading INFO.

    Returns:
        ``(connection, info, tls_established)``: the open connection, the
        parsed server INFO message, and whether the connection is currently
        over TLS.

    Raises:
        SecureConnectionRequiredError: ``wants_tls`` is set but the server
            advertises neither ``tls_required`` nor ``tls_available``.
        TimeoutError: transport open or INFO read didn't finish in ``timeout``.
        ConnectionError: any other transport-level failure.
    """
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or 4222
    scheme = parsed.scheme
    if not host:
        msg = f"Invalid URL: {url!r}"
        raise ValueError(msg)

    ssl_context: ssl.SSLContext | None = None
    if wants_tls:
        ssl_context = tls if tls is not None else ssl.create_default_context()
    server_hostname = tls_hostname if tls_hostname is not None else (host if ssl_context else None)

    tls_established = False
    try:
        if scheme in ("ws", "wss"):
            use_tls = scheme == "wss" or ssl_context is not None
            ws_url = url.replace("ws://", "wss://", 1) if scheme == "ws" and use_tls else url
            connection: Connection = await asyncio.wait_for(
                open_websocket_connection(
                    ws_url,
                    ssl_context=ssl_context if use_tls else None,
                    server_hostname=server_hostname if use_tls else None,
                ),
                timeout=timeout,
            )
            tls_established = use_tls
        elif tls_handshake_first and ssl_context is not None:
            connection = await asyncio.wait_for(
                open_tcp_connection(host, port, ssl_context=ssl_context, server_hostname=server_hostname),
                timeout=timeout,
            )
            tls_established = True
        else:
            connection = await asyncio.wait_for(
                open_tcp_connection(host, port),
                timeout=timeout,
            )
    except asyncio.TimeoutError:
        msg = f"Connection timed out after {timeout} seconds"
        raise TimeoutError(msg)
    except (SecureConnectionRequiredError, ConnectionError, TimeoutError):
        raise
    except Exception as e:
        msg = f"Failed to connect: {e}"
        raise ConnectionError(msg) from e

    try:
        protocol_message = await asyncio.wait_for(parse(connection), timeout=timeout)
        if not isinstance(protocol_message, Info):
            msg = "Expected INFO message"
            raise RuntimeError(msg)
        info: ProtocolServerInfo = protocol_message.info

        tls_required = info.get("tls_required", False)
        tls_available = info.get("tls_available", False)

        if wants_tls and not tls_established and not (tls_required or tls_available):
            await connection.close()
            raise SecureConnectionRequiredError

        if (wants_tls or tls_required) and not tls_established:
            upgrade_ssl_context = tls if tls is not None else ssl.create_default_context()
            upgrade_hostname = tls_hostname if tls_hostname is not None else host
            if isinstance(connection, TcpConnection):
                await connection.upgrade_to_tls(upgrade_ssl_context, upgrade_hostname)
                tls_established = True
            else:
                await connection.close()
                msg = "Server requires TLS but connection does not support upgrade"
                raise ConnectionError(msg)
    except SecureConnectionRequiredError:
        raise
    except (ConnectionError, TimeoutError):
        await connection.close()
        raise
    except Exception as e:
        await connection.close()
        msg = f"Failed to establish connection: {e}"
        raise ConnectionError(msg) from e

    return connection, info, tls_established
