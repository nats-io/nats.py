"""NATS client implementation.

This module provides a high-level, asyncio-based client for the NATS messaging system.
It implements core NATS features including:
- Publish/Subscribe messaging
- Request/Reply pattern
- Queue groups for load balancing
- Message headers
- Automatic reconnection
- Wildcard subscriptions

The primary entry point is the `connect()` function which returns a `Client` instance.
"""

from __future__ import annotations

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("nats-client")
except (ImportError, PackageNotFoundError):
    __version__ = "unknown"

import asyncio
import contextlib
import json
import logging
import random
import ssl
import uuid
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Self
from urllib.parse import urlparse

from nats.client.connection import Connection, open_tcp_connection
from nats.client.errors import NoRespondersError, SlowConsumerError, StatusError
from nats.client.message import Headers, Message, Status
from nats.client.protocol.command import (
    encode_connect,
    encode_hpub,
    encode_ping,
    encode_pong,
    encode_pub,
    encode_sub,
    encode_unsub,
)
from nats.client.protocol.message import ParseError, parse
from nats.client.protocol.types import (
    ConnectInfo,
)
from nats.client.protocol.types import (
    ServerInfo as ProtocolServerInfo,
)
from nats.client.subscription import Subscription

if TYPE_CHECKING:
    import types
    from collections.abc import Callable

logger = logging.getLogger("nats.client")


class ClientStatus(Enum):
    """Client connection status."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DRAINING = "draining"  # Draining subscribers
    DRAINED = "drained"  # Subscribers drained, flushing publishes
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class ServerInfo:
    """Server information received during connection."""

    server_id: str
    version: str
    go_version: str
    host: str
    port: int
    headers: bool
    auth_required: bool
    tls_required: bool
    tls_verify: bool
    max_payload: int
    proto: int
    client_id: int | None = None
    connect_urls: list[str] | None = None
    jetstream: bool | None = None
    nonce: str | None = None

    @classmethod
    def from_protocol(cls, info: ProtocolServerInfo) -> ServerInfo:
        """Create a ServerInfo instance from protocol info dictionary."""
        return cls(
            server_id=info["server_id"],
            version=info["version"],
            go_version=info["go"],
            host=info["host"],
            port=info["port"],
            headers=info["headers"],
            auth_required=info.get("auth_required", False),
            tls_required=info.get("tls_required", False),
            tls_verify=info.get("tls_verify", False),
            max_payload=info.get("max_payload", 1048576),
            proto=info.get("proto", 1),
            client_id=info.get("client_id"),
            connect_urls=info.get("connect_urls"),
            jetstream=info.get("jetstream"),
            nonce=info.get("nonce"),
        )


@dataclass(slots=True)
class ClientStatistics:
    """Statistics for messages and bytes sent/received on the connection.

    This is a snapshot of the connection statistics at a point in time.
    All fields are monotonically increasing counters.
    """

    in_msgs: int = 0
    """Number of incoming messages received."""

    out_msgs: int = 0
    """Number of outgoing messages published."""

    in_bytes: int = 0
    """Number of bytes received."""

    out_bytes: int = 0
    """Number of bytes sent."""

    reconnects: int = 0
    """Number of successful reconnection attempts."""


class Client(AbstractAsyncContextManager["Client"]):
    """High-level NATS client."""

    # Connection and server info
    _connection: Connection
    _server_info: ServerInfo
    _status: ClientStatus
    _last_error: str | None

    # Reconnection configuration
    _allow_reconnect: bool
    _reconnect_max_attempts: int
    _reconnect_time_wait: float
    _reconnect_time_wait_max: float
    _reconnect_jitter: float
    _reconnect_timeout: float
    _no_randomize: bool

    # Server pool management
    _server_pool: list[str]
    _last_server: str | None

    # Reconnection state
    _reconnecting: bool
    _reconnect_attempts: int
    _reconnect_time: float
    _reconnect_lock: asyncio.Lock

    # Subscriptions
    _subscriptions: dict[str, Subscription]
    _next_sid: int

    # Write buffering
    _pending_bytes: int
    _pending_messages: list[bytes]
    _max_pending_bytes: int
    _max_pending_messages: int
    _min_flush_interval: float
    _last_flush: float
    _flush_waker: asyncio.Event

    # Ping/Pong keep-alive
    _ping_interval: float
    _max_outstanding_pings: int
    _pings_outstanding: int
    _last_pong_received: float
    _last_ping_sent: float
    _pong_waker: asyncio.Event

    # Callbacks
    _disconnected_callbacks: list[Callable[[], None]]
    _reconnected_callbacks: list[Callable[[], None]]
    _error_callbacks: list[Callable[[str], None]]

    # Inbox prefix
    _inbox_prefix: str

    # Authentication
    _token: str | None
    _user: str | None
    _password: str | None
    _nkey_seed: str | None

    # TLS
    _tls: ssl.SSLContext | None
    _tls_hostname: str | None

    # Statistics
    _stats_in_msgs: int
    _stats_out_msgs: int
    _stats_in_bytes: int
    _stats_out_bytes: int
    _stats_reconnects: int

    # Background tasks
    _read_task: asyncio.Task[None]
    _write_task: asyncio.Task[None]

    def __init__(
        self,
        connection: Connection,
        server_info: ServerInfo,
        *,
        servers: list[str],
        allow_reconnect: bool = True,
        reconnect_max_attempts: int = 10,
        reconnect_time_wait: float = 2.0,
        reconnect_time_wait_max: float = 10.0,
        reconnect_jitter: float = 0.1,
        reconnect_timeout: float = 2.0,
        no_randomize: bool = False,
        inbox_prefix: str = "_INBOX",
        ping_interval: float = 120.0,
        max_outstanding_pings: int = 2,
        token: str | None = None,
        user: str | None = None,
        password: str | None = None,
        nkey_seed: str | None = None,
        tls: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
    ):
        """Initialize the client.

        Args:
            connection: NATS connection
            server_info: Server information
            servers: List of server addresses for the server pool
            allow_reconnect: Whether to automatically reconnect if the connection is lost
            reconnect_max_attempts: Maximum number of reconnection attempts (0 for unlimited)
            reconnect_time_wait: Initial wait time between reconnection attempts
            reconnect_time_wait_max: Maximum wait time between reconnection attempts
            reconnect_jitter: Jitter factor for reconnection attempts
            reconnect_timeout: Timeout for reconnection attempts
            no_randomize: Whether to disable randomizing the server pool
            inbox_prefix: Prefix for inbox subjects (default: "_INBOX")
            ping_interval: Interval between PINGs in seconds (default: 120.0)
            max_outstanding_pings: Maximum number of outstanding PINGs before disconnecting (default: 2)
            token: Authentication token for the server
            user: Username for authentication
            password: Password for authentication
            nkey_seed: NKey seed for authentication
            tls: SSL context for TLS connections
            tls_hostname: Hostname for TLS certificate verification
        """
        self._connection = connection
        self._server_info = server_info
        self._allow_reconnect = allow_reconnect
        self._reconnect_max_attempts = reconnect_max_attempts
        self._reconnect_time_wait = reconnect_time_wait
        self._reconnect_time_wait_max = reconnect_time_wait_max
        self._reconnect_jitter = reconnect_jitter
        self._reconnect_timeout = reconnect_timeout
        self._no_randomize = no_randomize

        # Validate inbox prefix (same rules as nats.go)
        if not inbox_prefix:
            raise ValueError("inbox_prefix cannot be empty")
        if ">" in inbox_prefix:
            raise ValueError("inbox_prefix cannot contain '>' wildcard")
        if "*" in inbox_prefix:
            raise ValueError("inbox_prefix cannot contain '*' wildcard")
        if inbox_prefix.endswith("."):
            raise ValueError("inbox_prefix cannot end with '.'")

        self._inbox_prefix = inbox_prefix
        self._token = token
        self._user = user
        self._password = password
        self._nkey_seed = nkey_seed
        self._tls = tls
        self._tls_hostname = tls_hostname
        self._status = ClientStatus.CONNECTING
        self._subscriptions = {}
        self._next_sid = 1
        self._last_error = None

        # Server pool management
        self._server_pool = servers

        # Reconnection state
        self._reconnect_attempts = 0
        self._reconnecting = False
        self._reconnect_time = self._reconnect_time_wait
        self._reconnect_lock = asyncio.Lock()
        self._last_server = None

        # Subscriptions
        self._pending_bytes = 0
        self._pending_messages = []
        self._max_pending_bytes = 1 * 1024 * 1024
        self._max_pending_messages = 1 * 512
        self._min_flush_interval = 0.005
        self._last_flush = asyncio.get_event_loop().time() - self._min_flush_interval
        self._flush_waker = asyncio.Event()

        # Ping/Pong keep-alive
        self._ping_interval = ping_interval
        self._max_outstanding_pings = max_outstanding_pings
        self._pings_outstanding = 0
        self._last_pong_received = asyncio.get_event_loop().time()
        self._last_ping_sent = self._last_pong_received
        self._pong_waker = asyncio.Event()

        # Callbacks
        self._disconnected_callbacks = []
        self._reconnected_callbacks = []
        self._error_callbacks = []

        # Statistics
        self._stats_in_msgs = 0
        self._stats_out_msgs = 0
        self._stats_in_bytes = 0
        self._stats_out_bytes = 0
        self._stats_reconnects = 0

        # Start background tasks
        self._read_task = asyncio.create_task(self._read_loop())
        self._write_task = asyncio.create_task(self._write_loop())

    @property
    def server_info(self) -> ServerInfo | None:
        """Get the server info received during connection."""
        return self._server_info

    @property
    def status(self) -> ClientStatus:
        """Get the current client status."""
        return self._status

    @property
    def last_error(self) -> str | None:
        """Get the last protocol error received from the server."""
        return self._last_error

    def stats(self) -> ClientStatistics:
        """Return a snapshot of the current connection statistics.

        Returns a copy of the statistics at the current point in time.
        All counters are monotonically increasing and represent totals
        since the connection was established.

        Returns:
            ClientStatistics: Snapshot of messages and bytes sent/received,
                and number of reconnections.
        """
        return ClientStatistics(
            in_msgs=self._stats_in_msgs,
            out_msgs=self._stats_out_msgs,
            in_bytes=self._stats_in_bytes,
            out_bytes=self._stats_out_bytes,
            reconnects=self._stats_reconnects,
        )

    async def _read_loop(self) -> None:
        """Background task that reads and processes incoming protocol messages."""
        try:
            while True:
                try:
                    msg = await parse(self._connection)

                    if not msg:
                        logger.info("Connection closed by server")
                        break

                    match msg:
                        case ("MSG", subject, sid, reply_to, payload):
                            logger.debug(
                                "<<- MSG %s %s %s %s", subject, sid, reply_to if reply_to else "", len(payload)
                            )
                            await self._handle_msg(subject, sid, reply_to, payload)
                        case ("HMSG", subject, sid, reply_to, headers, payload, status_code, status_description):
                            logger.debug("<<- HMSG %s %s %s %s %s", subject, sid, reply_to, len(headers), len(payload))
                            await self._handle_hmsg(
                                subject, sid, reply_to, headers, payload, status_code, status_description
                            )
                        case ("PING",):
                            logger.debug("<<- PING")
                            await self._handle_ping()
                        case ("PONG",):
                            logger.debug("<<- PONG")
                            await self._handle_pong()
                        case ("INFO", info):
                            logger.debug("<<- INFO %s...", json.dumps(info)[:80])
                            await self._handle_info(info)
                        case ("ERR", error):
                            logger.error("<<- -ERR '%s'", error)
                            await self._handle_error(error)
                except Exception:
                    logger.exception("Error in read loop")
                    break
        except (asyncio.CancelledError, ParseError) as e:
            logger.debug("Read loop exiting: %s", e)
            return

        await self._force_disconnect()

    async def _handle_ping(self) -> None:
        """Handle PING from server."""
        logger.debug("->> PONG")
        await self._connection.write(encode_pong())

    async def _handle_pong(self) -> None:
        """Handle PONG from server."""
        self._last_pong_received = asyncio.get_event_loop().time()
        self._pings_outstanding = 0
        self._pong_waker.set()

    async def _queue_ping(self) -> bool:
        """Queue a PING to be sent after the next flush.

        Returns:
            bool: True if a PING was queued, False if max outstanding PINGs reached.
        """
        if self._pings_outstanding >= self._max_outstanding_pings:
            logger.error("Max outstanding PINGs reached")
            await self._force_disconnect()
            return False

        self._pings_outstanding += 1
        self._last_ping_sent = asyncio.get_event_loop().time()
        await self._connection.write(encode_ping())
        return True

    async def _write_loop(self) -> None:
        """Background task that handles periodic flushes and PINGs."""
        try:
            while self._status == ClientStatus.CONNECTED:
                try:
                    try:
                        await asyncio.wait_for(self._flush_waker.wait(), timeout=self._ping_interval)
                        self._flush_waker.clear()

                        current_time = asyncio.get_event_loop().time()
                        since_last_flush = current_time - self._last_flush
                        if since_last_flush < self._min_flush_interval:
                            await asyncio.sleep(self._min_flush_interval - since_last_flush)

                        if self._pending_messages:
                            await self._force_flush()
                            self._last_flush = current_time

                    except asyncio.TimeoutError:
                        current_time = asyncio.get_event_loop().time()

                        if current_time - self._last_ping_sent >= self._ping_interval:
                            if self._pings_outstanding >= self._max_outstanding_pings:
                                logger.exception("Max outstanding PINGs reached")
                                await self._force_disconnect()
                                break

                            if self._pending_messages:
                                await self._force_flush()
                                self._last_flush = current_time

                            await self._queue_ping()

                except Exception:
                    logger.exception("Error in write loop")
                    if self._status != ClientStatus.CONNECTED:
                        break

        except asyncio.CancelledError:
            if self._pending_messages:
                try:
                    await self._force_flush()
                except Exception:
                    logger.exception("Error during final flush")
            return

    async def _handle_msg(self, subject: str, sid: str, reply_to: str | None, payload: bytes) -> None:
        """Handle MSG from server."""
        # Update statistics
        self._stats_in_msgs += 1
        self._stats_in_bytes += len(payload)

        if sid in self._subscriptions:
            subscription = self._subscriptions[sid]

            msg = Message(subject=subject, data=payload, reply_to=reply_to)

            try:
                # Try to enqueue message (handles limits and callbacks)
                subscription._enqueue(msg)

                # Reset slow consumer flag if we successfully queued
                if subscription._slow_consumer_reported:
                    subscription._slow_consumer_reported = False

            except (asyncio.QueueFull, ValueError):
                # Drop message due to limit exceeded
                msg_size = len(payload)
                subscription._dropped_messages += 1
                subscription._dropped_bytes += msg_size

                pending_msgs, pending_bytes = subscription.pending

                logger.warning(
                    "Slow consumer on subject %s (sid %s): dropping message, %d pending messages, %d pending bytes",
                    subject,
                    sid,
                    pending_msgs,
                    pending_bytes,
                )

                # Only report once per slow consumer event to avoid noise
                if not subscription._slow_consumer_reported:
                    subscription._slow_consumer_reported = True
                    error = SlowConsumerError(subject, sid, pending_msgs, pending_bytes)
                    for callback in self._error_callbacks:
                        try:
                            callback(error)
                        except Exception:
                            logger.exception("Error in error callback")

    async def _handle_hmsg(
        self,
        subject: str,
        sid: str,
        reply_to: str,
        headers: dict[str, list[str]],
        payload: bytes,
        status_code: str | None = None,
        status_description: str | None = None,
    ) -> None:
        """Handle HMSG from server."""
        # Update statistics
        self._stats_in_msgs += 1
        self._stats_in_bytes += len(payload)

        if sid in self._subscriptions:
            subscription = self._subscriptions[sid]

            status = None
            if status_code is not None:
                status = Status(code=status_code, description=status_description)

            msg = Message(
                subject=subject,
                data=payload,
                reply_to=reply_to,
                headers=Headers(headers) if headers else None,  # type: ignore[arg-type]
                status=status,
            )

            try:
                # Try to enqueue message (handles limits and callbacks)
                subscription._enqueue(msg)

                # Reset slow consumer flag if we successfully queued
                if subscription._slow_consumer_reported:
                    subscription._slow_consumer_reported = False

            except (asyncio.QueueFull, ValueError):
                # Drop message due to limit exceeded
                msg_size = len(payload)
                subscription._dropped_messages += 1
                subscription._dropped_bytes += msg_size

                pending_msgs, pending_bytes = subscription.pending

                logger.warning(
                    "Slow consumer on subject %s (sid %s): dropping message, %d pending messages, %d pending bytes",
                    subject,
                    sid,
                    pending_msgs,
                    pending_bytes,
                )

                # Only report once per slow consumer event to avoid noise
                if not subscription._slow_consumer_reported:
                    subscription._slow_consumer_reported = True
                    error = SlowConsumerError(subject, sid, pending_msgs, pending_bytes)
                    for callback in self._error_callbacks:
                        try:
                            callback(error)
                        except Exception:
                            logger.exception("Error in error callback")

    async def _handle_info(self, info: dict) -> None:
        """Handle INFO from server."""
        self._server_info = ServerInfo.from_protocol(info)
        # Update server pool with new cluster URLs from INFO
        if self._server_info.connect_urls:
            # Add new servers from connect_urls, avoiding duplicates
            for url in self._server_info.connect_urls:
                if url not in self._server_pool:
                    self._server_pool.append(url)

    async def _handle_error(self, error: str) -> None:
        """Handle ERR from server."""
        self._last_error = error

        if self._error_callbacks:
            for callback in self._error_callbacks:
                try:
                    callback(error)
                except Exception:
                    logger.exception("Error in error callback while handling server error: %s", error)

    async def _force_disconnect(self) -> None:
        """Force disconnect from server."""
        logger.info("Force disconnecting")

        old_status = self._status
        self._status = ClientStatus.DISCONNECTED
        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._read_task

        if self._write_task and not self._write_task.done():
            self._write_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._write_task

        if self._connection.is_connected():
            try:
                await self._connection.close()
            except Exception:
                logger.debug("Error closing connection", exc_info=True)

        # Use lock to prevent concurrent reconnection attempts
        async with self._reconnect_lock:
            if (
                old_status not in (ClientStatus.CLOSING, ClientStatus.CLOSED)
                and self._allow_reconnect
                and not self._reconnecting
            ):
                logger.info("Starting reconnection process")
                self._status = ClientStatus.RECONNECTING

                if self._disconnected_callbacks:
                    for callback in self._disconnected_callbacks:
                        try:
                            callback()
                        except Exception:
                            logger.exception("Error in disconnected callback")

                self._reconnecting = True
                self._reconnect_attempts = 0
                self._reconnect_time = self._reconnect_time_wait

                while self._reconnect_max_attempts == 0 or self._reconnect_attempts < self._reconnect_max_attempts:
                    if not self._allow_reconnect:
                        logger.info("Reconnection aborted - allow_reconnect flag disabled")
                        break

                    self._reconnect_attempts += 1
                    logger.info("Reconnection attempt %s", self._reconnect_attempts)

                    try:
                        actual_wait = self._reconnect_time * (1 + random.random() * self._reconnect_jitter)

                        logger.info("Waiting %.2fs before reconnection attempt", actual_wait)
                        await asyncio.sleep(actual_wait)

                        # Create a shuffled copy of the server pool if randomization is enabled
                        servers_to_try = self._server_pool.copy()
                        if not self._no_randomize and len(servers_to_try) > 1:
                            # Shuffle all but the first (original) server
                            tail = servers_to_try[1:]
                            random.shuffle(tail)
                            servers_to_try = [servers_to_try[0]] + tail

                        for server in servers_to_try:
                            if server == self._last_server and len(self._server_pool) > 1:
                                continue

                            logger.info("Trying to reconnect to %s", server)

                            if "://" in server:
                                parsed_url = urlparse(server)
                            else:
                                scheme = "tls" if self._server_info.tls_required else "nats"

                                if not server.startswith("[") and server.count(":") > 1:
                                    last_colon = server.rfind(":")
                                    try:
                                        port_val = int(server[last_colon + 1 :])
                                        if 0 <= port_val <= 65535:
                                            host_part = server[:last_colon]
                                            server = f"[{host_part}]:{port_val}"
                                    except ValueError:
                                        server = f"[{server}]"

                                parsed_url = urlparse(f"{scheme}://{server}")

                            host = parsed_url.hostname
                            port = parsed_url.port or 4222
                            scheme = parsed_url.scheme

                            if not host:
                                logger.warning("Failed to parse hostname from server URL: %s", server)
                                continue

                            try:
                                # Determine SSL context for reconnection
                                ssl_context = None
                                if scheme in ("tls", "wss"):
                                    # Use stored TLS context or create default
                                    ssl_context = self._tls if self._tls is not None else ssl.create_default_context()
                                elif self._tls is not None:
                                    # User explicitly provided TLS context
                                    ssl_context = self._tls

                                # Determine server hostname for TLS verification
                                server_hostname = (
                                    self._tls_hostname
                                    if self._tls_hostname is not None
                                    else (host if ssl_context else None)
                                )

                                # Connect with or without TLS
                                connection = await asyncio.wait_for(
                                    open_tcp_connection(
                                        host, port, ssl_context=ssl_context, server_hostname=server_hostname
                                    ),
                                    timeout=self._reconnect_timeout,
                                )

                                msg = await parse(connection)
                                if not msg or msg.op != "INFO":
                                    msg = "Expected INFO message"
                                    raise RuntimeError(msg)

                                new_server_info = ServerInfo.from_protocol(msg.info)
                                logger.info(
                                    "Reconnected to %s (version %s)", new_server_info.server_id, new_server_info.version
                                )

                                connect_info = ConnectInfo(
                                    verbose=False,
                                    pedantic=False,
                                    tls_required=False,
                                    lang="python",
                                    version=__version__,
                                    protocol=1,
                                    headers=True,
                                )

                                # Add authentication if provided
                                if self._token:
                                    connect_info["auth_token"] = self._token
                                if self._user:
                                    connect_info["user"] = self._user
                                if self._password:
                                    connect_info["password"] = self._password
                                if self._nkey_seed:
                                    import nkeys

                                    # Load the NKey from seed
                                    kp = nkeys.from_seed(self._nkey_seed.encode())

                                    # Add public key to connect info
                                    connect_info["nkey"] = kp.public_key.decode()

                                    # If server sent a nonce, sign it
                                    if new_server_info.nonce:
                                        sig = kp.sign(new_server_info.nonce.encode())
                                        import base64

                                        connect_info["sig"] = base64.b64encode(sig).decode()

                                logger.debug("->> CONNECT %s", json.dumps(connect_info))
                                await connection.write(encode_connect(connect_info))

                                self._connection = connection
                                self._server_info = new_server_info
                                self._status = ClientStatus.CONNECTED
                                self._last_server = server

                                # Update server pool with new cluster URLs after reconnection
                                if new_server_info.connect_urls:
                                    # Add new servers from connect_urls, avoiding duplicates
                                    for url in new_server_info.connect_urls:
                                        if url not in self._server_pool:
                                            self._server_pool.append(url)

                                for sid, subscription in list(self._subscriptions.items()):
                                    subject = subscription.subject
                                    queue_group = subscription.queue_group
                                    logger.debug("->> SUB %s %s %s", subject, sid, queue_group)
                                    await self._connection.write(encode_sub(subject, sid, queue_group))

                                await self._force_flush()

                                self._read_task = asyncio.create_task(self._read_loop())
                                self._write_task = asyncio.create_task(self._write_loop())

                                self._reconnecting = False
                                self._reconnect_attempts = 0
                                self._reconnect_time = self._reconnect_time_wait

                                # Update statistics
                                self._stats_reconnects += 1

                                if self._reconnected_callbacks:
                                    for callback in self._reconnected_callbacks:
                                        try:
                                            callback()
                                        except Exception:
                                            logger.exception("Error in reconnected callback")

                                return

                            except (asyncio.CancelledError, asyncio.TimeoutError) as e:
                                logger.error("Failed to connect to %s: %s", server, type(e).__name__)
                                self._last_server = server
                                continue
                            except Exception:
                                logger.exception("Failed to connect to %s", server)
                                self._last_server = server
                                continue

                        logger.error("Failed to connect to any server in the pool")

                        self._reconnect_time = min(self._reconnect_time * 2, self._reconnect_time_wait_max)

                    except Exception:
                        logger.exception("Reconnection attempt failed")

                logger.error("Reconnection failed after maximum attempts")
                self._reconnecting = False
                self._status = ClientStatus.CLOSED
            else:
                # Not attempting reconnection, set status to CLOSED
                self._status = ClientStatus.CLOSED

    async def _force_flush(self) -> None:
        """Flush pending messages to the server."""
        if not self._pending_messages:
            return

        # Check if we're connected before trying to write
        if not self._connection.is_connected():
            return

        await self._connection.write(b"".join(self._pending_messages))

        self._pending_messages.clear()
        self._pending_bytes = 0

    async def flush(self, timeout: float | None = None) -> None:
        """Flush pending messages with optional timeout."""
        if self._status == ClientStatus.CLOSED:
            logger.debug("Flush called on closed connection, skipping")
            return

        if not self._pending_messages:
            return

        await self._force_flush()

        self._pong_waker.clear()
        logger.debug("->> PING")
        self._pings_outstanding += 1
        self._last_ping_sent = asyncio.get_event_loop().time()
        await self._connection.write(encode_ping())
        try:
            await asyncio.wait_for(self._pong_waker.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.exception("PONG not received within timeout")
            await self._force_disconnect()

    async def publish(
        self,
        subject: str,
        payload: bytes,
        *,
        reply_to: str | None = None,
        headers: Headers | dict[str, str | list[str]] | None = None,
    ) -> None:
        """Publish a message to a subject."""
        if self._status in (ClientStatus.CLOSED, ClientStatus.CLOSING):
            msg = "Connection is closed"
            raise RuntimeError(msg)

        if headers:
            headers_dict = headers.asdict() if isinstance(headers, Headers) else headers
            command_parts = encode_hpub(
                subject,
                payload,
                reply_to=reply_to,
                headers=headers_dict,  # type: ignore[arg-type]
            )
        else:
            command_parts = encode_pub(
                subject,
                payload,
                reply_to=reply_to,
            )

        message_data = b"".join(command_parts)
        message_size = len(message_data)

        if (
            self._pending_bytes + message_size > self._max_pending_bytes
            or len(self._pending_messages) >= self._max_pending_messages
        ):
            await self._force_flush()

        self._pending_messages.append(message_data)
        self._pending_bytes += message_size

        # Update statistics
        self._stats_out_msgs += 1
        self._stats_out_bytes += len(payload)

        self._flush_waker.set()

    async def subscribe(
        self,
        subject: str,
        *,
        queue_group: str = "",
        max_pending_messages: int | None = 65536,
        max_pending_bytes: int | None = 67108864,  # 64 MB
    ) -> Subscription:
        """Subscribe to a subject.

        Args:
            subject: The subject to subscribe to
            queue_group: Optional queue group name for load balancing
            max_pending_messages: Maximum number of pending messages before triggering
                slow consumer error (default: 65536). Use None for unlimited.
            max_pending_bytes: Maximum bytes of pending messages before triggering
                slow consumer error (default: 64MB). Use None for unlimited.

        Returns:
            The subscription object

        Raises:
            RuntimeError: If the connection is closed
        """
        if self._status == ClientStatus.CLOSED:
            msg = "Connection is closed"
            raise RuntimeError(msg)

        sid = str(self._next_sid)
        self._next_sid += 1

        subscription = Subscription(
            subject,
            sid,
            queue_group,
            self,
            max_pending_messages=max_pending_messages,
            max_pending_bytes=max_pending_bytes,
        )

        self._subscriptions[sid] = subscription

        command = encode_sub(subject, sid, queue_group)
        if queue_group:
            logger.debug("->> SUB %s %s %s", subject, queue_group, sid)
        else:
            logger.debug("->> SUB %s %s", subject, sid)

        await self._connection.write(command)

        return subscription

    async def _subscribe(self, subject: str, sid: str, queue_group: str | None) -> asyncio.Queue:
        """Create a subscription on the server and return the message queue.

        Args:
            subject: The subject to subscribe to
            sid: The subscription ID
            queue_group: Optional queue group for load balancing

        Returns:
            An asyncio.Queue that will receive messages for this subscription
        """
        queue = asyncio.Queue()

        command = encode_sub(subject, sid, queue_group)
        if queue_group:
            logger.debug("->> SUB %s %s %s", subject, queue_group, sid)
        else:
            logger.debug("->> SUB %s %s", subject, sid)

        await self._connection.write(command)

        return queue

    async def _unsubscribe(self, sid: str) -> None:
        """Send UNSUB command to server for a subscription.

        Args:
            sid: Subscription ID
        """
        logger.debug("->> UNSUB %s", sid)

        if sid in self._subscriptions:
            if self._status not in (ClientStatus.CLOSED, ClientStatus.CLOSING):
                await self._connection.write(encode_unsub(sid))
            # Remove from subscriptions map
            del self._subscriptions[sid]

    def new_inbox(self) -> str:
        """Generate a new inbox subject.

        Returns:
            A unique inbox subject using the configured inbox prefix
        """
        return f"{self._inbox_prefix}.{uuid.uuid4().hex}"

    async def request(
        self,
        subject: str,
        payload: bytes,
        *,
        timeout: float = 2.0,
        headers: dict[str, str | list[str]] | None = None,
        return_on_error: bool = False,
    ) -> Message:
        """Send a request and wait for a response.

        Args:
            subject: The subject to send the request to
            payload: The request payload as bytes
            timeout: How long to wait for a response (default: 2.0 seconds)
            headers: Optional headers to include with the request
            return_on_error: If False (default), raises StatusError for error responses.
                           If True, returns the error response as a normal Message.

        Returns:
            The response message

        Raises:
            RuntimeError: If the connection is closed
            TimeoutError: If no response is received within the timeout
            StatusError: If return_on_error=False and the response contains error status headers
        """
        if self._status == ClientStatus.CLOSED:
            msg = "Connection is closed"
            raise RuntimeError(msg)

        inbox = self.new_inbox()
        logger.debug("Created inbox %s for request to %s", inbox, subject)

        sub = await self.subscribe(inbox)
        try:
            await self.publish(subject, payload, reply_to=inbox, headers=headers)

            try:
                response = await asyncio.wait_for(sub.next(), timeout)

                if not return_on_error and response.status is not None and response.status.code != "200":
                    status = response.status.code
                    description = response.status.description or "Unknown error"
                    raise StatusError.from_status(status, description, subject=subject)

                return response
            except asyncio.TimeoutError:
                logger.exception("Request timeout (%ss) on %s", timeout, subject)
                msg = "Request timeout"
                raise TimeoutError(msg)

        finally:
            await self._unsubscribe(sub._sid)

    async def drain(self, timeout: float = 30.0) -> None:
        """Drain the connection.

        Draining a connection puts it into a drain state where:
        1. All subscriptions are drained (unsubscribed but pending messages can be processed)
        2. No new messages can be published
        3. Pending messages in the write buffer are flushed
        4. The connection is closed

        This allows for graceful shutdown without losing messages. After drain completes,
        the connection will be closed automatically.

        This method is idempotent - calling it multiple times is safe and will not raise
        errors. Subsequent calls after the first will return immediately without error.

        Args:
            timeout: Maximum time to wait for drain to complete (default: 30.0 seconds)

        Raises:
            TimeoutError: If drain does not complete within the timeout
        """
        # Idempotent: if already draining, drained, closing, or closed, return without error
        if self._status in (ClientStatus.DRAINING, ClientStatus.DRAINED, ClientStatus.CLOSING, ClientStatus.CLOSED):
            return

        logger.info("Draining connection")
        self._status = ClientStatus.DRAINING

        # Disable reconnection during drain
        self._allow_reconnect = False

        try:
            # Step 1: Drain all subscriptions (DRAINING phase)
            # Get a snapshot of current subscriptions to avoid modification during iteration
            subscriptions_to_drain = list(self._subscriptions.values())

            if subscriptions_to_drain:
                logger.debug("Draining %s subscriptions", len(subscriptions_to_drain))
                drain_tasks = [sub.drain() for sub in subscriptions_to_drain]
                await asyncio.wait_for(asyncio.gather(*drain_tasks, return_exceptions=True), timeout=timeout)

            # Step 2: Transition to DRAINED and flush pending publishes
            self._status = ClientStatus.DRAINED

            if self._pending_messages:
                logger.debug("Flushing pending messages")
                await asyncio.wait_for(self.flush(), timeout=timeout)

            # Step 3: Close the connection
            await self.close()

        except asyncio.TimeoutError:
            logger.error("Drain timeout after %s seconds", timeout)
            # Force close on timeout
            await self.close()
            msg = f"Drain operation timed out after {timeout} seconds"
            raise TimeoutError(msg)

    async def close(self) -> None:
        """Close the connection."""
        if self._status == ClientStatus.CLOSED:
            return

        logger.info("Closing connection")
        self._status = ClientStatus.CLOSING

        self._allow_reconnect = False

        if self._read_task and isinstance(self._read_task, asyncio.Task) and not self._read_task.done():
            self._read_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._read_task

        if self._write_task and isinstance(self._write_task, asyncio.Task) and not self._write_task.done():
            self._write_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._write_task

        subscription_count = len(self._subscriptions)
        if subscription_count > 0:
            logger.debug("Closing %s subscriptions", subscription_count)

            sids = list(self._subscriptions.keys())
            for sid in sids:
                if sid in self._subscriptions:
                    subscription = self._subscriptions[sid]
                    await subscription.unsubscribe()

        try:
            await self._connection.close()
        except Exception:
            logger.exception("Error closing connection during force disconnect")

        self._flush_waker.set()

        tasks_to_cancel = []
        if self._read_task and not self._read_task.done():
            tasks_to_cancel.append(self._read_task)
            self._read_task.cancel()

        if self._write_task and not self._write_task.done():
            tasks_to_cancel.append(self._write_task)
            self._write_task.cancel()

        if tasks_to_cancel:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        self._status = ClientStatus.CLOSED

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None
    ) -> None:
        """Exit the async context manager, closing the client connection."""
        await self.close()

    def add_disconnected_callback(self, callback: Callable[[], None]) -> None:
        """Add a callback to be invoked when the client is disconnected.

        Args:
            callback: Function to be called when disconnected
        """
        self._disconnected_callbacks.append(callback)

    def add_reconnected_callback(self, callback: Callable[[], None]) -> None:
        """Add a callback to be invoked when the client is reconnected.

        Args:
            callback: Function to be called when reconnected
        """
        self._reconnected_callbacks.append(callback)

    def add_error_callback(self, callback: Callable[[str], None]) -> None:
        """Add a callback to be invoked when the client encounters an error.

        Args:
            callback: Function to be called with the error message
        """
        self._error_callbacks.append(callback)

    async def _send_connect(self) -> None:
        """Send CONNECT message to the server."""
        connect_info = ConnectInfo(
            verbose=False,
            pedantic=False,
            tls_required=False,
            lang="python",
            version=__version__,
            protocol=1,
            headers=True,
        )

        # Add authentication if provided
        if self._token:
            connect_info["auth_token"] = self._token
        if self._user:
            connect_info["user"] = self._user
        if self._password:
            connect_info["password"] = self._password
        if self._nkey_seed:
            import nkeys

            # Load the NKey from seed
            kp = nkeys.from_seed(self._nkey_seed.encode())

            # Add public key to connect info
            connect_info["nkey"] = kp.public_key.decode()

            # If server sent a nonce, sign it
            if self._server_info.nonce:
                sig = kp.sign(self._server_info.nonce.encode())
                import base64

                connect_info["sig"] = base64.b64encode(sig).decode()

        logger.debug("->> CONNECT %s", json.dumps(connect_info))
        await self._connection.write(encode_connect(connect_info))
        self._status = ClientStatus.CONNECTED


async def connect(
    url: str = "nats://localhost:4222",
    *,
    timeout: float = 2.0,
    tls: ssl.SSLContext | None = None,
    tls_hostname: str | None = None,
    tls_handshake_first: bool = False,
    allow_reconnect: bool = True,
    reconnect_max_attempts: int = 10,
    reconnect_time_wait: float = 2.0,
    reconnect_time_wait_max: float = 10.0,
    reconnect_jitter: float = 0.1,
    reconnect_timeout: float | None = None,
    no_randomize: bool = False,
    inbox_prefix: str = "_INBOX",
    ping_interval: float = 120.0,
    max_outstanding_pings: int = 2,
    token: str | None = None,
    user: str | None = None,
    password: str | None = None,
    nkey_seed: str | None = None,
) -> Client:
    """Connect to a NATS server.

    Args:
        url: Server URL
        timeout: Connection timeout in seconds
        tls: Custom SSL context for TLS connections (uses default if scheme is tls://)
        tls_hostname: Override hostname for TLS certificate verification
        tls_handshake_first: Perform TLS handshake before receiving INFO message
        allow_reconnect: Whether to automatically reconnect if the connection is lost
        reconnect_max_attempts: Maximum number of reconnection attempts (0 for unlimited)
        reconnect_time_wait: Initial wait time between reconnection attempts
        reconnect_time_wait_max: Maximum wait time between reconnection attempts
        reconnect_jitter: Jitter factor for reconnection attempts
        reconnect_timeout: Timeout for individual reconnection attempts (defaults to timeout value)
        no_randomize: Whether to disable randomizing the server pool
        inbox_prefix: Prefix for inbox subjects (default: "_INBOX")
        ping_interval: Interval between PINGs in seconds (default: 120.0)
        max_outstanding_pings: Maximum number of outstanding PINGs before disconnecting (default: 2)
        token: Authentication token for the server
        user: Username for authentication
        password: Password for authentication
        nkey_seed: NKey seed for authentication

    Returns:
        Client instance

    Raises:
        TimeoutError: Connection timed out
        ConnectionError: Failed to connect
        ValueError: Invalid URL
    """
    # Parse URL
    parsed_url = urlparse(url)
    if parsed_url.scheme not in ("nats", "tls", "ws", "wss"):
        msg = "URL scheme must be 'nats://', 'tls://', 'ws://', or 'wss://'"
        raise ValueError(msg)

    host = parsed_url.hostname or "localhost"
    port = parsed_url.port or 4222

    logger.info("Connecting to %s:%s", host, port)

    # Determine SSL context
    ssl_context = None
    if parsed_url.scheme in ("tls", "wss"):
        # Use provided SSL context or create default
        ssl_context = tls if tls is not None else ssl.create_default_context()
    elif tls is not None:
        # User explicitly provided TLS context, use it even for nats:// scheme
        ssl_context = tls

    # Determine server hostname for TLS verification
    server_hostname = tls_hostname if tls_hostname is not None else (host if ssl_context else None)

    # Open connection with timeout
    # Track whether we've actually established TLS yet
    tls_established = False
    try:
        if tls_handshake_first and ssl_context:
            # TLS handshake first mode - establish TLS before reading INFO
            connection = await asyncio.wait_for(
                open_tcp_connection(host, port, ssl_context=ssl_context, server_hostname=server_hostname),
                timeout=timeout,
            )
            tls_established = True
        else:
            # Plain connection - may upgrade to TLS after receiving INFO
            connection = await asyncio.wait_for(
                open_tcp_connection(host, port),
                timeout=timeout,
            )
    except asyncio.TimeoutError:
        msg = f"Connection timed out after {timeout} seconds"
        raise TimeoutError(msg)
    except Exception as e:
        msg = f"Failed to connect: {e}"
        raise ConnectionError(msg)

    # Parse server INFO message
    try:
        msg = await parse(connection)
        if not msg or msg.op != "INFO":
            msg = "Expected INFO message"
            raise RuntimeError(msg)

        server_info = ServerInfo.from_protocol(msg.info)
        logger.info("Connected to %s (version %s)", server_info.server_id, server_info.version)

        # Check if server requires TLS upgrade and we haven't established TLS yet
        if server_info.tls_required and not tls_established:
            logger.info("Server requires TLS, upgrading connection")
            # Create SSL context if not provided
            upgrade_ssl_context = tls if tls is not None else ssl.create_default_context()
            upgrade_hostname = tls_hostname if tls_hostname is not None else host

            # Upgrade the connection to TLS
            if hasattr(connection, "upgrade_to_tls"):
                await connection.upgrade_to_tls(upgrade_ssl_context, upgrade_hostname)
                # Update our tracking
                ssl_context = upgrade_ssl_context
                server_hostname = upgrade_hostname
                tls_established = True
            else:
                await connection.close()
                msg = "Server requires TLS but connection does not support upgrade"
                raise ConnectionError(msg)

    except Exception as e:
        await connection.close()
        msg = f"Failed to connect: {e}"
        raise ConnectionError(msg)

    # Build server pool: start with the URL we connected to, then add cluster URLs
    servers = [f"{host}:{port}"]
    if server_info.connect_urls:
        servers.extend(server_info.connect_urls)

    # Send CONNECT message (complete handshake before creating Client)
    connect_info = ConnectInfo(
        verbose=False,
        pedantic=False,
        tls_required=tls_established,  # Tell server we're using TLS
        lang="python",
        version=__version__,
        protocol=1,
        headers=True,
        no_responders=True,
    )

    # Add authentication if provided
    if token:
        connect_info["auth_token"] = token
    if user:
        connect_info["user"] = user
    if password:
        connect_info["password"] = password
    if nkey_seed:
        import nkeys

        # Load the NKey from seed
        kp = nkeys.from_seed(nkey_seed.encode())

        # Add public key to connect info
        connect_info["nkey"] = kp.public_key.decode()

        # If server sent a nonce, sign it
        if server_info.nonce:
            sig = kp.sign(server_info.nonce.encode())
            import base64

            connect_info["sig"] = base64.b64encode(sig).decode()

    logger.debug("->> CONNECT %s", json.dumps(connect_info))
    await connection.write(encode_connect(connect_info))

    # Send a PING and wait for PONG to verify the handshake completed successfully
    # If auth fails, the server will send -ERR before we get a PONG
    await connection.write(encode_ping())

    # Wait for response to verify connection is good
    try:
        response = await asyncio.wait_for(parse(connection), timeout=timeout)

        # Check if we got an error response
        if response and response.op == "ERR":
            await connection.close()
            error_msg = response.error

            # Check for authorization errors
            if "authorization" in error_msg.lower():
                msg = f"Authorization failed: {error_msg}"
                raise ConnectionError(msg)
            else:
                msg = f"Connection error: {error_msg}"
                raise ConnectionError(msg)

        # If we got PONG or INFO or any other non-error message, connection is good
        # (Server may send additional INFO messages after CONNECT)

    except asyncio.TimeoutError:
        await connection.close()
        msg = "Server did not respond to PING"
        raise ConnectionError(msg)
    except ConnectionError:
        # Re-raise connection errors from error checking above
        raise
    except Exception as e:
        await connection.close()
        msg = f"Failed to verify connection: {e}"
        raise ConnectionError(msg)

    # Handshake complete - now create the Client with background tasks
    # Store the TLS context that was used (original or created during upgrade)
    client = Client(
        connection,
        server_info,
        servers=servers,
        allow_reconnect=allow_reconnect,
        reconnect_max_attempts=reconnect_max_attempts,
        reconnect_time_wait=reconnect_time_wait,
        reconnect_time_wait_max=reconnect_time_wait_max,
        reconnect_jitter=reconnect_jitter,
        reconnect_timeout=reconnect_timeout if reconnect_timeout is not None else timeout,
        no_randomize=no_randomize,
        inbox_prefix=inbox_prefix,
        ping_interval=ping_interval,
        max_outstanding_pings=max_outstanding_pings,
        token=token,
        user=user,
        password=password,
        nkey_seed=nkey_seed,
        tls=ssl_context if ssl_context else tls,  # Use actual context if TLS was used
        tls_hostname=server_hostname if server_hostname else tls_hostname,
    )

    client._status = ClientStatus.CONNECTED

    return client


__all__ = [
    "__version__",
    "Message",
    "Headers",
    "Status",
    "Subscription",
    "Client",
    "ServerInfo",
    "ClientStatus",
    "ClientStatistics",
    "StatusError",
    "NoRespondersError",
]
