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

    __version__ = version("nats-core")
except (ImportError, PackageNotFoundError):
    __version__ = "unknown"

import asyncio
import base64
import contextlib
import json
import logging
import random
import re
import ssl
import uuid
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self, TypeAlias
from urllib.parse import urlparse

import nkeys
from nats.client.connection import Connection, establish_connection
from nats.client.errors import (
    MaxPayloadError,
    NoRespondersError,
    SecureConnectionRequiredError,
    SlowConsumerError,
    StatusError,
)
from nats.client.message import Headers, Message, Status
from nats.client.protocol.command import (
    encode_connect,
    encode_headers,
    encode_hpub,
    encode_ping,
    encode_pong,
    encode_pub,
    encode_sub,
    encode_unsub,
)
from nats.client.protocol.message import Err, Ok, ParseError, Pong, parse
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

_DEFAULT_PENDING_BYTES_LIMIT: Final[int] = 1024 * 1024
_DEFAULT_PENDING_MESSAGES_LIMIT: Final[int] = 512
_DEFAULT_MIN_FLUSH_INTERVAL: Final[float] = 0.005


NkeyPublicKeyHandler: TypeAlias = Callable[[], str]
"""Handler that returns the NKey public key."""

NkeySignatureHandler: TypeAlias = Callable[[str], bytes]
"""Handler that signs a nonce and returns the signature."""

NkeySeed: TypeAlias = str | Path
"""NKey seed as string or path to seed file."""

NkeyHandlers: TypeAlias = tuple[NkeyPublicKeyHandler, NkeySignatureHandler]
"""Custom NKey handlers for full control over authentication."""

JWTHandler: TypeAlias = Callable[[], bytes]
"""Handler that returns the JWT."""

JWTSignatureHandler: TypeAlias = Callable[[str], bytes]
"""Handler that signs a nonce and returns the signature."""

JWTCredentials: TypeAlias = Path | tuple[str, str] | tuple[Path, Path]
"""JWT credentials as .creds file, (jwt_string, seed_string), or (jwt_file, seed_file)."""

JWTHandlers: TypeAlias = tuple[JWTHandler, JWTSignatureHandler]
"""Custom JWT handlers for full control over authentication."""


class ClientStatus(Enum):
    """Client connection status."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DRAINING = "draining"
    DRAINED = "drained"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class ServerInfo:
    """Server information received during connection."""

    server_id: str
    server_name: str
    version: str
    go_version: str
    host: str
    port: int
    headers: bool
    auth_required: bool
    tls_required: bool
    tls_available: bool
    tls_verify: bool
    max_payload: int
    proto: int
    client_id: int | None = None
    connect_urls: list[str] | None = None
    jetstream: bool | None = None
    nonce: str | None = None
    lame_duck_mode: bool = False

    @classmethod
    def from_protocol(cls, info: ProtocolServerInfo) -> ServerInfo:
        """Create a ServerInfo instance from protocol info dictionary."""
        return cls(
            server_id=info["server_id"],
            server_name=info.get("server_name", ""),
            version=info["version"],
            go_version=info["go"],
            host=info["host"],
            port=info["port"],
            headers=info["headers"],
            auth_required=info.get("auth_required", False),
            tls_required=info.get("tls_required", False),
            tls_available=info.get("tls_available", False),
            tls_verify=info.get("tls_verify", False),
            max_payload=info.get("max_payload", 1048576),
            proto=info.get("proto", 1),
            client_id=info.get("client_id"),
            connect_urls=info.get("connect_urls"),
            jetstream=info.get("jetstream"),
            nonce=info.get("nonce"),
            lame_duck_mode=info.get("ldm", False),
        )


@dataclass(slots=True)
class ClientStatistics:
    """Statistics for messages and bytes sent/received on the connection.

    This is a snapshot of the connection statistics at a point in time.
    All fields are monotonically increasing counters.
    """

    in_messages: int = 0
    """Number of incoming messages received."""

    out_messages: int = 0
    """Number of outgoing messages published."""

    in_bytes: int = 0
    """Number of bytes received."""

    out_bytes: int = 0
    """Number of bytes sent."""

    reconnects: int = 0
    """Number of successful reconnection attempts."""


_SUBJECT_INVALID_RE = re.compile(r"[ \t\r\n]")


def _validate_subject(subject: str | bytes, *, strict: bool = False) -> str:
    """Validate a NATS subject and return the str form.

    Always rejects empty subjects, non-UTF-8 bytes (via UnicodeDecodeError, a
    ValueError subclass), and whitespace or CRLF. CRLF in particular would
    allow a caller to inject arbitrary protocol commands.

    With ``strict=True`` (the subscribe path), also rejects empty tokens and
    misplaced wildcards — `nats.py`-specific structural checks that go
    beyond what `nats.go`/`nats.rs` enforce client-side. The publish path
    leaves token shape to the server to stay compatible with the reference
    clients.
    """
    if isinstance(subject, bytes):
        subject = subject.decode("utf-8")
    if not subject:
        raise ValueError("subject cannot be empty")
    if _SUBJECT_INVALID_RE.search(subject):
        raise ValueError(f"subject cannot contain whitespace or CRLF: {subject!r}")
    if not strict:
        return subject
    tokens = subject.split(".")
    for index, token in enumerate(tokens):
        if not token:
            raise ValueError(f"subject cannot contain empty tokens: {subject!r}")
        if token == ">":
            if index != len(tokens) - 1:
                raise ValueError(f"'>' wildcard must be the last token: {subject!r}")
        elif ">" in token:
            raise ValueError(f"'>' is only valid as a whole token: {subject!r}")
        elif "*" in token and token != "*":
            raise ValueError(f"'*' is only valid as a whole token: {subject!r}")
    return subject


def _validate_queue(queue: str | bytes) -> str:
    """Validate a NATS queue group name and return the str form.

    Empty queue is treated as unset. Matches `nats.go`'s ``badQueue``: only
    whitespace and CRLF are rejected. Wildcards and dots are valid tokens
    on the wire (``workers.east``, ``workers.*``).
    """
    if isinstance(queue, bytes):
        queue = queue.decode("utf-8")
    if not queue:
        return queue
    if _SUBJECT_INVALID_RE.search(queue):
        raise ValueError(f"queue cannot contain whitespace or CRLF: {queue!r}")
    return queue


class Client(AbstractAsyncContextManager["Client"]):
    """High-level NATS client."""

    _connection: Connection
    _server_info: ServerInfo
    _status: ClientStatus
    _last_error: str | None

    # Reconnection
    _allow_reconnect: bool
    _reconnect_max_attempts: int
    _reconnect_time_wait: float
    _reconnect_time_wait_max: float
    _reconnect_jitter: float
    _reconnect_timeout: float
    _no_randomize: bool
    _no_echo: bool
    _server_pool: list[str]
    _last_server: str | None
    _reconnecting: bool
    _reconnect_attempts: int
    _reconnect_time: float
    _reconnect_lock: asyncio.Lock
    _reconnect_wake: asyncio.Event

    # Subscriptions
    _subscriptions: dict[str, Subscription]
    _next_sid: int

    # Request multiplexer (SID "0")
    _request_prefix: str | None
    _request_futures: dict[int, asyncio.Future[Message]]
    _next_request_id: int

    # Write buffering
    _pending_bytes: int
    _pending_messages: list[bytes]
    _max_pending_bytes: int
    _max_pending_messages: int
    _min_flush_interval: float
    _last_flush: float
    _flush_waker: asyncio.Event

    # Ping/pong
    _ping_interval: float
    _max_outstanding_pings: int
    _pings_outstanding: int
    _last_pong_received: float
    _last_ping_sent: float
    _pong_waker: asyncio.Event

    # Callbacks
    _disconnected_callbacks: list[Callable[[], None]]
    _reconnected_callbacks: list[Callable[[], None]]
    _error_callbacks: list[Callable[[Exception | str], None]]
    _lame_duck_mode_callbacks: list[Callable[[], None]]

    # Inbox
    _inbox_prefix: str

    # Connection label
    _name: str | None

    # Authentication
    _token: str | Callable[[], str] | None
    _user: str | Callable[[], str] | None
    _password: str | Callable[[], str] | None
    _nkey_public_key_handler: Callable[[], str] | None
    _nkey_signature_handler: Callable[[str], bytes] | None
    _jwt_handler: Callable[[], bytes] | None
    _jwt_signature_handler: Callable[[str], bytes] | None

    # TLS
    _tls: ssl.SSLContext | None
    _tls_hostname: str | None
    _tls_handshake_first: bool
    _wants_tls: bool

    # CONNECT protocol options
    _verbose: bool
    _pedantic: bool

    # Subject validation
    _skip_subject_validation: bool

    # Statistics
    _stats_in_messages: int
    _stats_out_messages: int
    _stats_in_bytes: int
    _stats_out_bytes: int
    _stats_reconnects: int

    # Tasks
    _read_task: asyncio.Task[None]
    _write_task: asyncio.Task[None]

    def __init__(
        self,
        connection: Connection,
        server_info: ServerInfo,
        *,
        servers: list[str],
        allow_reconnect: bool = True,
        reconnect_max_attempts: int = 60,
        reconnect_time_wait: float = 2.0,
        reconnect_time_wait_max: float = 10.0,
        reconnect_jitter: float = 0.1,
        reconnect_timeout: float = 2.0,
        no_randomize: bool = False,
        no_echo: bool = False,
        inbox_prefix: str = "_INBOX",
        ping_interval: float = 120.0,
        max_outstanding_pings: int = 2,
        name: str | None = None,
        token: str | Callable[[], str] | None = None,
        user: str | Callable[[], str] | None = None,
        password: str | Callable[[], str] | None = None,
        nkey_public_key_handler: Callable[[], str] | None = None,
        nkey_signature_handler: Callable[[str], bytes] | None = None,
        jwt_handler: Callable[[], bytes] | None = None,
        jwt_signature_handler: Callable[[str], bytes] | None = None,
        tls: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
        wants_tls: bool = False,
        verbose: bool = False,
        pedantic: bool = False,
        skip_subject_validation: bool = False,
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
            no_echo: If True, the server will not send messages published by this connection back to it
            inbox_prefix: Prefix for inbox subjects (default: "_INBOX")
            ping_interval: Interval between PINGs in seconds (default: 120.0)
            max_outstanding_pings: Maximum number of outstanding PINGs before disconnecting (default: 2)
            name: Optional client label sent as the ``name`` field in CONNECT
            token: Authentication token for the server
            user: Username for authentication
            password: Password for authentication
            nkey_public_key_handler: Handler to get nkey public key
            nkey_signature_handler: Handler to sign nonces with nkey
            jwt_handler: Handler to get JWT
            jwt_signature_handler: Handler to sign nonces for JWT auth
            tls: SSL context for TLS connections
            tls_hostname: Hostname for TLS certificate verification
            tls_handshake_first: Perform the TLS handshake before receiving INFO
            wants_tls: Whether the client requested TLS (via scheme, tls context, or tls_handshake_first)
            verbose: If True, the server will reply +OK on each protocol message (default: False)
            pedantic: If True, the server enforces strict protocol checks (default: False)
            skip_subject_validation: If True, skip client-side subject and queue validation
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
        self._no_echo = no_echo

        if not inbox_prefix:
            raise ValueError("inbox_prefix cannot be empty")
        if ">" in inbox_prefix:
            raise ValueError("inbox_prefix cannot contain '>' wildcard")
        if "*" in inbox_prefix:
            raise ValueError("inbox_prefix cannot contain '*' wildcard")
        if inbox_prefix.endswith("."):
            raise ValueError("inbox_prefix cannot end with '.'")

        self._inbox_prefix = inbox_prefix
        self._name = name
        self._token = token
        self._user = user
        self._password = password
        self._nkey_public_key_handler = nkey_public_key_handler
        self._nkey_signature_handler = nkey_signature_handler
        self._jwt_handler = jwt_handler
        self._jwt_signature_handler = jwt_signature_handler
        self._tls = tls
        self._tls_hostname = tls_hostname
        self._tls_handshake_first = tls_handshake_first
        self._wants_tls = wants_tls
        self._verbose = verbose
        self._pedantic = pedantic
        self._skip_subject_validation = skip_subject_validation
        self._status = ClientStatus.CONNECTING
        self._subscriptions = {}
        self._next_sid = 1
        self._request_prefix = None
        self._request_futures = {}
        self._next_request_id = 0
        self._last_error = None
        self._server_pool = servers
        self._reconnect_attempts = 0
        self._reconnecting = False
        self._reconnect_time = self._reconnect_time_wait
        self._reconnect_lock = asyncio.Lock()
        self._reconnect_wake = asyncio.Event()
        self._last_server = None
        self._pending_bytes = 0
        self._pending_messages = []
        self._max_pending_bytes = _DEFAULT_PENDING_BYTES_LIMIT
        self._max_pending_messages = _DEFAULT_PENDING_MESSAGES_LIMIT
        self._min_flush_interval = _DEFAULT_MIN_FLUSH_INTERVAL
        loop = asyncio.get_running_loop()
        self._last_flush = loop.time() - self._min_flush_interval
        self._flush_waker = asyncio.Event()
        self._ping_interval = ping_interval
        self._max_outstanding_pings = max_outstanding_pings
        self._pings_outstanding = 0
        self._last_pong_received = loop.time()
        self._last_ping_sent = self._last_pong_received
        self._pong_waker = asyncio.Event()
        self._disconnected_callbacks = []
        self._reconnected_callbacks = []
        self._error_callbacks = []
        self._lame_duck_mode_callbacks = []
        self._stats_in_messages = 0
        self._stats_out_messages = 0
        self._stats_in_bytes = 0
        self._stats_out_bytes = 0
        self._stats_reconnects = 0
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
            in_messages=self._stats_in_messages,
            out_messages=self._stats_out_messages,
            in_bytes=self._stats_in_bytes,
            out_bytes=self._stats_out_bytes,
            reconnects=self._stats_reconnects,
        )

    async def _read_loop(self) -> None:
        """Background task that reads and processes incoming protocol messages."""
        try:
            while True:
                try:
                    protocol_message = await parse(self._connection)

                    if not protocol_message:
                        logger.info("Connection closed by server")
                        break

                    match protocol_message:
                        case ("MSG", subject, sid, reply, payload):
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug("<<- MSG %s %s %s %s", subject, sid, reply if reply else "", len(payload))
                            await self._handle_msg(subject, sid, reply, payload)
                        case ("HMSG", subject, sid, reply, headers, payload, status_code, status_description):
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug("<<- HMSG %s %s %s %s %s", subject, sid, reply, len(headers), len(payload))
                            await self._handle_hmsg(
                                subject, sid, reply, headers, payload, status_code, status_description
                            )
                        case ("PING",):
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug("<<- PING")
                            await self._handle_ping()
                        case ("PONG",):
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug("<<- PONG")
                            await self._handle_pong()
                        case ("INFO", info):
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug("<<- INFO %s...", json.dumps(info)[:80])
                            await self._handle_info(info)
                        case ("ERR", error):
                            logger.error("<<- -ERR '%s'", error)
                            await self._handle_error(error)
                        case ("OK",):
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug("<<- +OK")
                except Exception:
                    logger.exception("Error in read loop")
                    break
        except (asyncio.CancelledError, ParseError) as e:
            logger.debug("Read loop exiting: %s", e)
            return

        await self._force_disconnect()

    async def _handle_ping(self) -> None:
        """Handle PING from server."""
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("->> PONG")
        await self._connection.write(encode_pong())

    async def _handle_pong(self) -> None:
        """Handle PONG from server."""
        self._last_pong_received = asyncio.get_running_loop().time()
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
        self._last_ping_sent = asyncio.get_running_loop().time()
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

                        current_time = asyncio.get_running_loop().time()
                        since_last_flush = current_time - self._last_flush
                        if since_last_flush < self._min_flush_interval:
                            await asyncio.sleep(self._min_flush_interval - since_last_flush)

                        if self._pending_messages:
                            await self._force_flush()
                            self._last_flush = current_time

                    except TimeoutError:
                        current_time = asyncio.get_running_loop().time()

                        if current_time - self._last_ping_sent >= self._ping_interval:
                            if self._pings_outstanding >= self._max_outstanding_pings:
                                logger.error("Max outstanding PINGs reached")
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

    async def _handle_msg(self, subject: str, sid: str, reply: str | None, payload: bytes) -> None:
        """Handle MSG from server."""
        self._stats_in_messages += 1
        self._stats_in_bytes += len(payload)

        if sid == "0":
            assert self._request_prefix is not None
            try:
                token = int(subject[len(self._request_prefix) :])
            except (ValueError, TypeError):
                return
            future = self._request_futures.pop(token, None)
            if future is not None and not future.done():
                future.set_result(Message(subject=subject, data=payload, reply=reply))
            return

        if sid in self._subscriptions:
            subscription = self._subscriptions[sid]

            message = Message(subject=subject, data=payload, reply=reply)

            try:
                subscription._enqueue(message)

                if subscription._slow_consumer_reported:
                    subscription._slow_consumer_reported = False

            except (asyncio.QueueFull, ValueError):
                message_size = len(payload)
                subscription._dropped_messages += 1
                subscription._dropped_bytes += message_size

                pending_messages, pending_bytes = subscription.pending

                logger.warning(
                    "Slow consumer on subject %s (sid %s): dropping message, %d pending messages, %d pending bytes",
                    subject,
                    sid,
                    pending_messages,
                    pending_bytes,
                )

                if not subscription._slow_consumer_reported:
                    subscription._slow_consumer_reported = True
                    error = SlowConsumerError(subject, sid, pending_messages, pending_bytes)
                    for callback in self._error_callbacks:
                        try:
                            callback(error)
                        except Exception:
                            logger.exception("Error in error callback")

                # The server counts dropped messages against the UNSUB <sid> <max_messages>
                # cap, so a slow consumer can reach the cap without _delivered ever
                # catching up. Close locally to avoid leaving the subscription stuck
                # open after the server has stopped delivering.
                if (
                    subscription._max_messages is not None
                    and subscription._delivered + subscription._dropped_messages >= subscription._max_messages
                ):
                    subscription._close_local(immediate=False)

    async def _handle_hmsg(
        self,
        subject: str,
        sid: str,
        reply: str,
        headers: dict[str, list[str]],
        payload: bytes,
        status_code: str | None = None,
        status_description: str | None = None,
    ) -> None:
        """Handle HMSG from server."""
        self._stats_in_messages += 1
        self._stats_in_bytes += len(payload)

        if sid == "0":
            assert self._request_prefix is not None
            try:
                token = int(subject[len(self._request_prefix) :])
            except (ValueError, TypeError):
                return
            future = self._request_futures.pop(token, None)
            if future is not None and not future.done():
                status = None
                if status_code is not None:
                    status = Status(code=status_code, description=status_description)
                future.set_result(
                    Message(
                        subject=subject,
                        data=payload,
                        reply=reply,
                        headers=Headers(headers) if headers else None,  # type: ignore[arg-type]
                        status=status,
                    )
                )
            return

        if sid in self._subscriptions:
            subscription = self._subscriptions[sid]

            status = None
            if status_code is not None:
                status = Status(code=status_code, description=status_description)

            message = Message(
                subject=subject,
                data=payload,
                reply=reply,
                headers=Headers(headers) if headers else None,  # type: ignore[arg-type]
                status=status,
            )

            try:
                subscription._enqueue(message)

                if subscription._slow_consumer_reported:
                    subscription._slow_consumer_reported = False

            except (asyncio.QueueFull, ValueError):
                message_size = len(payload)
                subscription._dropped_messages += 1
                subscription._dropped_bytes += message_size

                pending_messages, pending_bytes = subscription.pending

                logger.warning(
                    "Slow consumer on subject %s (sid %s): dropping message, %d pending messages, %d pending bytes",
                    subject,
                    sid,
                    pending_messages,
                    pending_bytes,
                )

                if not subscription._slow_consumer_reported:
                    subscription._slow_consumer_reported = True
                    error = SlowConsumerError(subject, sid, pending_messages, pending_bytes)
                    for callback in self._error_callbacks:
                        try:
                            callback(error)
                        except Exception:
                            logger.exception("Error in error callback")

                # The server counts dropped messages against the UNSUB <sid> <max_messages>
                # cap, so a slow consumer can reach the cap without _delivered ever
                # catching up. Close locally to avoid leaving the subscription stuck
                # open after the server has stopped delivering.
                if (
                    subscription._max_messages is not None
                    and subscription._delivered + subscription._dropped_messages >= subscription._max_messages
                ):
                    subscription._close_local(immediate=False)

    async def _handle_info(self, info: ProtocolServerInfo) -> None:
        """Handle INFO from server."""
        was_lame_duck_mode = self._server_info.lame_duck_mode
        self._server_info = ServerInfo.from_protocol(info)
        if self._server_info.connect_urls:
            for url in self._server_info.connect_urls:
                if url not in self._server_pool:
                    self._server_pool.append(url)

        if self._server_info.lame_duck_mode and not was_lame_duck_mode:
            logger.info("Server entered lame duck mode")
            for callback in self._lame_duck_mode_callbacks:
                try:
                    callback()
                except Exception:
                    logger.exception("Error in lame duck mode callback")

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
                        self._reconnect_wake.clear()
                        with contextlib.suppress(TimeoutError):
                            await asyncio.wait_for(self._reconnect_wake.wait(), timeout=actual_wait)

                        servers_to_try = self._server_pool.copy()
                        if not self._no_randomize and len(servers_to_try) > 1:
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
                                scheme = "tls" if self._wants_tls or self._server_info.tls_required else "nats"

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
                            scheme = parsed_url.scheme

                            if not host:
                                logger.warning("Failed to parse hostname from server URL: %s", server)
                                continue

                            try:
                                wants_tls = self._wants_tls or scheme in ("tls", "wss")

                                connection, info, tls_established = await establish_connection(
                                    parsed_url.geturl(),
                                    timeout=self._reconnect_timeout,
                                    wants_tls=wants_tls,
                                    tls=self._tls,
                                    tls_hostname=self._tls_hostname,
                                    tls_handshake_first=self._tls_handshake_first,
                                )
                                new_server_info = ServerInfo.from_protocol(info)
                                logger.info(
                                    "Reconnected to %s (version %s)", new_server_info.server_id, new_server_info.version
                                )

                                connect_info = ConnectInfo(
                                    verbose=self._verbose,
                                    pedantic=self._pedantic,
                                    tls_required=tls_established,
                                    lang="python",
                                    version=__version__,
                                    protocol=1,
                                    headers=True,
                                    no_responders=True,
                                    echo=not self._no_echo,
                                )

                                if self._name is not None:
                                    connect_info["name"] = self._name

                                if self._token:
                                    connect_info["auth_token"] = self._token() if callable(self._token) else self._token
                                if self._user:
                                    connect_info["user"] = self._user() if callable(self._user) else self._user
                                if self._password:
                                    connect_info["password"] = (
                                        self._password() if callable(self._password) else self._password
                                    )

                                if self._jwt_handler is not None:
                                    connect_info["jwt"] = self._jwt_handler().decode()
                                    if new_server_info.nonce and self._jwt_signature_handler is not None:
                                        connect_info["sig"] = self._jwt_signature_handler(
                                            new_server_info.nonce
                                        ).decode()
                                elif self._nkey_public_key_handler is not None:
                                    connect_info["nkey"] = self._nkey_public_key_handler()
                                    if new_server_info.nonce and self._nkey_signature_handler is not None:
                                        connect_info["sig"] = self._nkey_signature_handler(
                                            new_server_info.nonce
                                        ).decode()

                                await connection.write(encode_connect(connect_info))
                                await connection.write(encode_ping())

                                try:
                                    while True:
                                        response = await asyncio.wait_for(
                                            parse(connection), timeout=self._reconnect_timeout
                                        )
                                        if not isinstance(response, Ok):
                                            break
                                except TimeoutError:
                                    await connection.close()
                                    msg = "Server did not respond to PING"
                                    raise ConnectionError(msg)

                                if response is None:
                                    await connection.close()
                                    msg = "Connection closed before PONG received"
                                    raise ConnectionError(msg)

                                if isinstance(response, Err):
                                    await connection.close()
                                    msg = f"Connection error: {response.error}"
                                    raise ConnectionError(msg)

                                if not isinstance(response, Pong):
                                    await connection.close()
                                    msg = f"Unexpected response to PING: {type(response).__name__}"
                                    raise ConnectionError(msg)

                                self._connection = connection
                                self._server_info = new_server_info
                                self._status = ClientStatus.CONNECTED
                                self._last_server = server

                                if new_server_info.connect_urls:
                                    for url in new_server_info.connect_urls:
                                        if url not in self._server_pool:
                                            self._server_pool.append(url)

                                for sid, subscription in list(self._subscriptions.items()):
                                    # If the subscription had an auto-unsubscribe cap and
                                    # has already received enough messages, drop it instead
                                    # of resending — matches nats.go's resendSubscriptions.
                                    remaining = None
                                    if subscription._max_messages is not None:
                                        # Dropped messages count against the cap — the old server
                                        # already routed them — so they reduce what the new server
                                        # may send, keeping the local close condition
                                        # (delivered + dropped >= cap) in agreement.
                                        remaining = (
                                            subscription._max_messages
                                            - subscription._delivered
                                            - subscription._dropped_messages
                                        )
                                        if remaining <= 0:
                                            subscription._close_local(immediate=False)
                                            continue

                                    subject = subscription.subject
                                    queue = subscription.queue
                                    logger.debug("->> SUB %s %s %s", subject, sid, queue)
                                    await self._connection.write(encode_sub(subject, sid, queue))

                                    if remaining is not None:
                                        logger.debug("->> UNSUB %s %d", sid, remaining)
                                        await self._connection.write(encode_unsub(sid, max_messages=remaining))

                                if self._request_prefix is not None:
                                    mux_subject = f"{self._request_prefix}*"
                                    await self._connection.write(encode_sub(mux_subject, "0"))

                                await self._force_flush()

                                self._read_task = asyncio.create_task(self._read_loop())
                                self._write_task = asyncio.create_task(self._write_loop())

                                self._reconnecting = False
                                self._reconnect_attempts = 0
                                self._reconnect_time = self._reconnect_time_wait
                                self._stats_reconnects += 1

                                if self._reconnected_callbacks:
                                    for callback in self._reconnected_callbacks:
                                        try:
                                            callback()
                                        except Exception:
                                            logger.exception("Error in reconnected callback")

                                return

                            except SecureConnectionRequiredError:
                                # TLS intent is a configuration error, not a per-server failure;
                                # propagate out of the reconnect loop instead of silently bypassing.
                                raise
                            except (asyncio.CancelledError, TimeoutError) as e:
                                logger.error("Failed to connect to %s: %s", server, type(e).__name__)
                                self._last_server = server
                                continue
                            except Exception:
                                logger.exception("Failed to connect to %s", server)
                                self._last_server = server
                                continue

                        logger.error("Failed to connect to any server in the pool")

                        self._reconnect_time = min(self._reconnect_time * 2, self._reconnect_time_wait_max)

                    except SecureConnectionRequiredError:
                        raise
                    except Exception:
                        logger.exception("Reconnection attempt failed")

                logger.error("Reconnection failed after maximum attempts")
                self._reconnecting = False
                self._status = ClientStatus.CLOSED
            else:
                self._status = ClientStatus.CLOSED

    async def _force_flush(self) -> None:
        """Flush pending messages to the server."""
        if not self._pending_messages:
            return

        if not self._connection.is_connected():
            return

        await self._connection.write(b"".join(self._pending_messages))

        self._pending_messages.clear()
        self._pending_bytes = 0

    async def _ping(self) -> None:
        """Send a PING to the server."""
        self._pong_waker.clear()
        logger.debug("->> PING")
        self._pings_outstanding += 1
        self._last_ping_sent = asyncio.get_running_loop().time()
        await self._connection.write(encode_ping())

    async def rtt(self, timeout: float | None = None) -> float:
        """Calculate the round trip time between the client and server in seconds."""
        if self._status == ClientStatus.CLOSED:
            raise ConnectionError("connection is closed")

        loop = asyncio.get_running_loop()
        start = loop.time()
        await self._ping()
        await asyncio.wait_for(self._pong_waker.wait(), timeout=timeout)
        return loop.time() - start

    async def flush(self, timeout: float | None = None) -> None:
        """Flush pending messages with optional timeout."""
        if self._status == ClientStatus.CLOSED:
            logger.debug("Flush called on closed connection, skipping")
            return

        if self._pending_messages:
            await self._force_flush()

        await self._ping()
        try:
            await asyncio.wait_for(self._pong_waker.wait(), timeout=timeout)
        except TimeoutError:
            logger.error("PONG not received within timeout")
            await self._force_disconnect()

    async def publish(
        self,
        subject: str | bytes,
        payload: bytes,
        *,
        reply: str | bytes | None = None,
        headers: Headers | dict[str, str | list[str]] | None = None,
    ) -> None:
        """Publish a message to a subject.

        Args:
            subject: Subject to publish to (str or bytes for zero-copy optimization)
            payload: Message payload
            reply: Optional reply subject (str or bytes for zero-copy optimization)
            headers: Optional message headers

        Raises:
            RuntimeError: Connection is closed.
            MaxPayloadError: The payload is larger than the server's ``max_payload``.
        """
        if self._status in (ClientStatus.CLOSED, ClientStatus.CLOSING):
            msg = "Connection is closed"
            raise RuntimeError(msg)

        if self._skip_subject_validation:
            subject = subject.encode() if isinstance(subject, str) else subject
            if reply:
                reply = reply.encode() if isinstance(reply, str) else reply
            else:
                reply = None
        else:
            subject = _validate_subject(subject).encode()
            if reply:
                reply = _validate_subject(reply).encode()
            else:
                reply = None

        # HPUB is what the server measures against max_payload, so include the
        # encoded header block in the size check. Encode headers once and reuse.
        header_data: bytes | None = None
        if headers:
            headers_dict = headers.asdict() if isinstance(headers, Headers) else headers
            header_data = encode_headers(headers_dict)  # type: ignore[arg-type]
            size = len(header_data) + len(payload)
        else:
            size = len(payload)

        max_payload = self._server_info.max_payload
        if max_payload > 0 and size > max_payload:
            raise MaxPayloadError(size, max_payload)

        if header_data is not None:
            message_data = encode_hpub(
                subject,
                payload,
                reply=reply,
                header_data=header_data,
            )
        else:
            message_data = encode_pub(
                subject,
                payload,
                reply=reply,
            )
        message_size = len(message_data)

        if (
            self._pending_bytes + message_size > self._max_pending_bytes
            or len(self._pending_messages) >= self._max_pending_messages
        ):
            await self._force_flush()

        self._pending_messages.append(message_data)
        self._pending_bytes += message_size

        self._stats_out_messages += 1
        self._stats_out_bytes += len(payload)

        self._flush_waker.set()

    async def subscribe(
        self,
        subject: str | bytes,
        *,
        queue: str | bytes = "",
        max_pending_messages: int | None = 65536,
        max_pending_bytes: int | None = 67108864,
    ) -> Subscription:
        """Subscribe to a subject.

        Args:
            subject: The subject to subscribe to
            queue: Optional queue group name for load balancing
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

        if self._skip_subject_validation:
            subject_str = subject.decode("utf-8") if isinstance(subject, bytes) else subject
            queue_str = queue.decode("utf-8") if isinstance(queue, bytes) else queue
        else:
            subject_str = _validate_subject(subject, strict=True)
            queue_str = _validate_queue(queue)

        sid = str(self._next_sid)
        self._next_sid += 1

        subscription = Subscription(
            subject_str,
            sid,
            queue_str,
            self,
            max_pending_messages=max_pending_messages,
            max_pending_bytes=max_pending_bytes,
        )

        self._subscriptions[sid] = subscription

        command = encode_sub(subject_str, sid, queue_str if queue_str else None)
        if queue_str:
            logger.debug("->> SUB %s %s %s", subject_str, queue_str, sid)
        else:
            logger.debug("->> SUB %s %s", subject_str, sid)

        await self._connection.write(command)

        return subscription

    async def _subscribe(self, subject: str, sid: str, queue: str | None = None) -> None:
        """Send a SUB command to the server.

        Args:
            subject: The subject to subscribe to
            sid: The subscription ID
            queue: Optional queue group for load balancing
        """
        command = encode_sub(subject, sid, queue)
        if queue:
            logger.debug("->> SUB %s %s %s", subject, queue, sid)
        else:
            logger.debug("->> SUB %s %s", subject, sid)

        await self._connection.write(command)

    async def _unsubscribe(self, sid: str, *, max_messages: int | None = None, keep: bool = False) -> None:
        """Send UNSUB command to server for a subscription.

        Args:
            sid: Subscription ID
            max_messages: If set, send ``UNSUB <sid> <max_messages>`` so the server
                stops delivering after that many messages instead of
                unsubscribing immediately.
            keep: If True, leave the subscription registered locally. Used by
                :meth:`Subscription.unsubscribe_after` so messages continue to
                be dispatched until the local cap is hit.
        """
        if max_messages is not None:
            logger.debug("->> UNSUB %s %d", sid, max_messages)
        else:
            logger.debug("->> UNSUB %s", sid)

        if sid in self._subscriptions:
            if self._status not in (ClientStatus.CLOSED, ClientStatus.CLOSING):
                await self._connection.write(encode_unsub(sid, max_messages=max_messages))
            if not keep:
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

        if not self._skip_subject_validation:
            _validate_subject(subject)

        if self._request_prefix is None:
            self._request_prefix = f"{self._inbox_prefix}.{uuid.uuid4().hex}."
            try:
                await self._subscribe(f"{self._request_prefix}*", "0")
            except Exception:
                self._request_prefix = None
                raise

        token = self._next_request_id
        self._next_request_id += 1
        inbox = f"{self._request_prefix}{token}"
        future: asyncio.Future[Message] = asyncio.Future()
        self._request_futures[token] = future

        try:
            await self.publish(subject, payload, reply=inbox, headers=headers)

            try:
                response = await asyncio.wait_for(future, timeout)

                if not return_on_error and response.status is not None and response.status.code != "200":
                    status = response.status.code
                    description = response.status.description or "Unknown error"
                    raise StatusError.from_status(status, description, subject=subject)

                return response
            except TimeoutError:
                logger.error("Request timeout (%ss) on %s", timeout, subject)
                msg = "Request timeout"
                raise TimeoutError(msg)

        finally:
            self._request_futures.pop(token, None)

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
            subscriptions_to_drain = list(self._subscriptions.values())

            if subscriptions_to_drain:
                logger.debug("Draining %s subscriptions", len(subscriptions_to_drain))
                drain_tasks = [sub.drain() for sub in subscriptions_to_drain]
                await asyncio.wait_for(asyncio.gather(*drain_tasks, return_exceptions=True), timeout=timeout)

            self._status = ClientStatus.DRAINED

            if self._pending_messages:
                logger.debug("Flushing pending messages")
                await asyncio.wait_for(self.flush(), timeout=timeout)

            await self.close()

        except TimeoutError:
            logger.error("Drain timeout after %s seconds", timeout)
            await self.close()
            msg = f"Drain operation timed out after {timeout} seconds"
            raise TimeoutError(msg)

    async def force_reconnect(self) -> None:
        """Force a reconnect to the server.

        If a reconnect cycle is already underway (status is ``DISCONNECTED`` or
        ``RECONNECTING``) the current backoff sleep is woken so the next attempt
        fires immediately; the call returns without waiting for the cycle to
        finish. From the ``CONNECTED`` state the existing connection is torn
        down and the call blocks until the reconnect loop completes — either
        landing on a server or exhausting attempts.

        Raises:
            ConnectionError: If the client is closed or draining.
            RuntimeError: If ``allow_reconnect=False``.
        """
        if self._status in (
            ClientStatus.CLOSING,
            ClientStatus.CLOSED,
            ClientStatus.DRAINING,
            ClientStatus.DRAINED,
        ):
            msg = "Connection is closed"
            raise ConnectionError(msg)
        if not self._allow_reconnect:
            msg = "Cannot force reconnect: allow_reconnect is disabled"
            raise RuntimeError(msg)

        # Reconnect cycle already in flight (including the brief window after
        # the read loop sets DISCONNECTED but before _force_disconnect sets
        # _reconnecting) — kick the backoff sleep so the next attempt fires
        # immediately instead of starting a second cycle.
        if self._reconnecting or self._status in (ClientStatus.DISCONNECTED, ClientStatus.RECONNECTING):
            self._reconnect_wake.set()
            return

        # Connected — close and let the normal reconnect flow run.
        await self._force_disconnect()

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

        for future in self._request_futures.values():
            if not future.done():
                future.cancel()
        self._request_futures.clear()

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
        except BaseException:
            logger.debug("Error closing connection during close", exc_info=True)

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

    def remove_disconnected_callback(self, callback: Callable[[], None]) -> None:
        """Remove a previously registered disconnected callback.

        Raises ``ValueError`` if ``callback`` was not registered.

        Args:
            callback: Function previously passed to :meth:`add_disconnected_callback`.
        """
        self._disconnected_callbacks.remove(callback)

    def add_reconnected_callback(self, callback: Callable[[], None]) -> None:
        """Add a callback to be invoked when the client is reconnected.

        Args:
            callback: Function to be called when reconnected
        """
        self._reconnected_callbacks.append(callback)

    def remove_reconnected_callback(self, callback: Callable[[], None]) -> None:
        """Remove a previously registered reconnected callback.

        Raises ``ValueError`` if ``callback`` was not registered.

        Args:
            callback: Function previously passed to :meth:`add_reconnected_callback`.
        """
        self._reconnected_callbacks.remove(callback)

    def add_error_callback(self, callback: Callable[[Exception | str], None]) -> None:
        """Add a callback to be invoked when the client encounters an error.

        Args:
            callback: Function to be called with the error
        """
        self._error_callbacks.append(callback)

    def remove_error_callback(self, callback: Callable[[Exception | str], None]) -> None:
        """Remove a previously registered error callback.

        Raises ``ValueError`` if ``callback`` was not registered.

        Args:
            callback: Function previously passed to :meth:`add_error_callback`.
        """
        self._error_callbacks.remove(callback)

    def add_lame_duck_mode_callback(self, callback: Callable[[], None]) -> None:
        """Add a callback to be invoked when the server enters lame duck mode.

        Fires once per transition, when an asynchronous INFO update flips ``ldm``
        from false to true. The server gradually evicts clients during its
        configured lame duck window; the normal reconnect path then runs on close.
        Applications can use this callback to drain in-flight work or log the
        event before the server closes the connection.

        If the server is already in lame duck mode when the client first
        connects, no transition is observed and the callback is not invoked —
        check ``client.server_info.lame_duck_mode`` after ``connect()`` returns
        to detect that case.

        Args:
            callback: Function to be called when the server enters lame duck mode.
        """
        self._lame_duck_mode_callbacks.append(callback)

    def remove_lame_duck_mode_callback(self, callback: Callable[[], None]) -> None:
        """Remove a previously registered lame duck mode callback.

        Raises ``ValueError`` if ``callback`` was not registered.

        Args:
            callback: Function previously passed to :meth:`add_lame_duck_mode_callback`.
        """
        self._lame_duck_mode_callbacks.remove(callback)


def _setup_nkey_auth(
    nkey: str | Path | tuple[Callable[[], str], Callable[[str], bytes]],
) -> tuple[Callable[[], str], Callable[[str], bytes]]:
    """Setup nkey authentication handlers from various input formats.

    Args:
        nkey: Nkey seed string, Path to seed file, or tuple of handlers

    Returns:
        Tuple of (public_key_handler, signature_handler)
    """
    if isinstance(nkey, tuple):
        # Already handlers, return as-is
        return nkey

    # Load seed from string or file
    if isinstance(nkey, Path):
        seed_bytes = nkey.read_bytes().strip()
    else:
        seed_bytes = nkey.encode()

    # Create handlers from seed
    def public_key_handler() -> str:
        kp = nkeys.from_seed(seed_bytes)
        return kp.public_key.decode()

    def signature_handler(nonce: str) -> bytes:
        kp = nkeys.from_seed(seed_bytes)
        sig = kp.sign(nonce.encode())
        return base64.urlsafe_b64encode(sig).rstrip(b"=")

    return public_key_handler, signature_handler


def _setup_jwt_auth(
    jwt: tuple[str, str] | Path | tuple[Path, Path] | tuple[Callable[[], bytes], Callable[[str], bytes]],
) -> tuple[Callable[[], bytes], Callable[[str], bytes]]:
    """Setup JWT authentication handlers from various input formats.

    Args:
        jwt: JWT config as (jwt_string, seed_string), Path to .creds file,
             (jwt_file, seed_file), or tuple of handlers

    Returns:
        Tuple of (jwt_handler, signature_handler)
    """
    if isinstance(jwt, tuple) and callable(jwt[0]):
        # Already handlers, return as-is
        return jwt  # type: ignore[return-value]

    # Parse JWT and seed
    jwt_content: bytes
    seed_bytes: bytes

    if isinstance(jwt, Path):
        # Single .creds file
        creds_content = jwt.read_text()

        # Extract JWT
        jwt_match = re.search(
            r"-----BEGIN NATS USER JWT-----\s*(.+?)\s*------END NATS USER JWT------",
            creds_content,
            re.DOTALL,
        )
        if not jwt_match:
            msg = f"No JWT found in credentials file: {jwt}"
            raise ValueError(msg)
        jwt_content = jwt_match.group(1).strip().encode()

        # Extract seed
        seed_match = re.search(
            r"-----BEGIN USER NKEY SEED-----\s*(.+?)\s*------END USER NKEY SEED-----",
            creds_content,
            re.DOTALL,
        )
        if not seed_match:
            msg = f"No seed found in credentials file: {jwt}"
            raise ValueError(msg)
        seed_bytes = seed_match.group(1).strip().encode()

    elif isinstance(jwt, tuple) and isinstance(jwt[0], Path) and isinstance(jwt[1], Path):
        # Separate files
        jwt_file: Path = jwt[0]
        seed_file: Path = jwt[1]
        jwt_content = jwt_file.read_bytes().strip()
        seed_bytes = seed_file.read_bytes().strip()

    elif isinstance(jwt, tuple):
        # Strings
        jwt_str, seed_str = jwt[0], jwt[1]
        assert isinstance(jwt_str, str)
        assert isinstance(seed_str, str)
        jwt_content = jwt_str.encode()
        seed_bytes = seed_str.encode()

    else:
        msg = f"Invalid jwt argument: {jwt!r}"
        raise TypeError(msg)

    # Create handlers
    def jwt_handler() -> bytes:
        return jwt_content

    def signature_handler(nonce: str) -> bytes:
        kp = nkeys.from_seed(seed_bytes)
        sig = kp.sign(nonce.encode())
        return base64.urlsafe_b64encode(sig).rstrip(b"=")

    return jwt_handler, signature_handler


async def connect(
    url: str = "nats://localhost:4222",
    *,
    timeout: float = 2.0,
    tls: ssl.SSLContext | None = None,
    tls_hostname: str | None = None,
    tls_handshake_first: bool = False,
    allow_reconnect: bool = True,
    reconnect_max_attempts: int = 60,
    reconnect_time_wait: float = 2.0,
    reconnect_time_wait_max: float = 10.0,
    reconnect_jitter: float = 0.1,
    reconnect_timeout: float | None = None,
    no_randomize: bool = False,
    no_echo: bool = False,
    inbox_prefix: str = "_INBOX",
    ping_interval: float = 120.0,
    max_outstanding_pings: int = 2,
    name: str | None = None,
    token: str | Callable[[], str] | None = None,
    user: str | Callable[[], str] | None = None,
    password: str | Callable[[], str] | None = None,
    nkey: NkeySeed | NkeyHandlers | None = None,
    jwt: JWTCredentials | JWTHandlers | None = None,
    verbose: bool = False,
    pedantic: bool = False,
    skip_subject_validation: bool = False,
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
        no_echo: If True, the server will not send messages published by this connection back to it (default: False)
        inbox_prefix: Prefix for inbox subjects (default: "_INBOX")
        ping_interval: Interval between PINGs in seconds (default: 120.0)
        max_outstanding_pings: Maximum number of outstanding PINGs before disconnecting (default: 2)
        name: Optional client label sent as the ``name`` field in CONNECT
        token: Authentication token for the server
        user: Username for authentication
        password: Password for authentication
        nkey: NKey authentication (bare nkey, no JWT). See `Nkey` type alias for options:
            - str: seed string (e.g., "SUAMLK2ZNL35...")
            - Path: path to seed file
            - tuple[NkeyPublicKeyHandler, NkeySignatureHandler]: custom handlers for full control
        jwt: JWT + NKey authentication. See `JWT` type alias for options:
            - tuple[str, str]: (jwt_string, seed_string)
            - Path: single .creds file containing both JWT and seed
            - tuple[Path, Path]: (jwt_file, seed_file)
            - tuple[JWTHandler, JWTSignatureHandler]: custom handlers for full control
        verbose: If True, the server will reply +OK on each protocol message (default: False)
        pedantic: If True, the server enforces strict protocol checks (default: False)
        skip_subject_validation: If True, disable client-side subject and queue validation
            on publish, subscribe, and request. Mirrors nats.go's ``SkipSubjectValidation``.
            WARNING: this disables CRLF-injection protection — only enable for hot-path
            benchmarks where you fully control the subject inputs.

    Returns:
        Client instance

    Raises:
        TimeoutError: Connection timed out
        ConnectionError: Failed to connect
        ValueError: Invalid URL
    """
    parsed_url = urlparse(url)
    if parsed_url.scheme not in ("nats", "tls", "ws", "wss"):
        msg = "URL scheme must be 'nats://', 'tls://', 'ws://', or 'wss://'"
        raise ValueError(msg)

    host = parsed_url.hostname or "localhost"
    port = parsed_url.port or 4222

    # URL-embedded credentials act as defaults for unset arguments.
    # Username with no password is treated as a token, matching the Go client.
    if parsed_url.username is not None and parsed_url.password is None:
        if token is None:
            token = parsed_url.username
    else:
        if user is None and parsed_url.username is not None:
            user = parsed_url.username
        if password is None and parsed_url.password is not None:
            password = parsed_url.password

    logger.info("Connecting to %s:%s", host, port)

    wants_tls = parsed_url.scheme in ("tls", "wss") or tls is not None or tls_handshake_first

    # Resolve the default SSL context once so it gets cached on Client and
    # reused on reconnect rather than rebuilt against the OS trust store each time.
    if wants_tls and tls is None:
        tls = ssl.create_default_context()

    connection, info, tls_established = await establish_connection(
        url,
        timeout=timeout,
        wants_tls=wants_tls,
        tls=tls,
        tls_hostname=tls_hostname,
        tls_handshake_first=tls_handshake_first,
    )
    server_info = ServerInfo.from_protocol(info)
    logger.info("Connected to %s (version %s)", server_info.server_id, server_info.version)

    # Preserve the WebSocket scheme in the reconnect pool, promoting ws:// → wss://
    # when TLS was applied during establish_connection. For TCP/TLS we only need
    # host:port — the scheme is implicit.
    if parsed_url.scheme in ("ws", "wss"):
        pool_url = url
        if parsed_url.scheme == "ws" and tls_established:
            pool_url = url.replace("ws://", "wss://", 1)
        servers = [pool_url]
    else:
        servers = [f"{host}:{port}"]
    if server_info.connect_urls:
        servers.extend(server_info.connect_urls)

    connect_info = ConnectInfo(
        verbose=verbose,
        pedantic=pedantic,
        tls_required=tls_established,
        lang="python",
        version=__version__,
        protocol=1,
        headers=True,
        no_responders=True,
        echo=not no_echo,
    )

    # Setup authentication handlers
    nkey_public_key_handler = None
    nkey_signature_handler = None
    jwt_handler = None
    jwt_signature_handler = None

    if nkey is not None:
        nkey_public_key_handler, nkey_signature_handler = _setup_nkey_auth(nkey)

    if jwt is not None:
        jwt_handler, jwt_signature_handler = _setup_jwt_auth(jwt)

    if name is not None:
        connect_info["name"] = name

    # Resolve callables for token/user/password
    resolved_token = token() if callable(token) else token
    resolved_user = user() if callable(user) else user
    resolved_password = password() if callable(password) else password

    # Apply authentication to CONNECT message
    if resolved_token:
        connect_info["auth_token"] = resolved_token
    if resolved_user:
        connect_info["user"] = resolved_user
    if resolved_password:
        connect_info["password"] = resolved_password

    if jwt_handler is not None:
        # JWT authentication
        connect_info["jwt"] = jwt_handler().decode()
        if server_info.nonce and jwt_signature_handler is not None:
            connect_info["sig"] = jwt_signature_handler(server_info.nonce).decode()
    elif nkey_public_key_handler is not None:
        # Bare nkey authentication
        connect_info["nkey"] = nkey_public_key_handler()
        if server_info.nonce and nkey_signature_handler is not None:
            connect_info["sig"] = nkey_signature_handler(server_info.nonce).decode()

    await connection.write(encode_connect(connect_info))

    await connection.write(encode_ping())

    try:
        while True:
            response = await asyncio.wait_for(parse(connection), timeout=timeout)
            if not isinstance(response, Ok):
                break

        if isinstance(response, Err):
            await connection.close()
            error_msg = response.error

            if "authorization" in error_msg.lower():
                msg = f"Authorization failed: {error_msg}"
                raise ConnectionError(msg)
            else:
                msg = f"Connection error: {error_msg}"
                raise ConnectionError(msg)

    except TimeoutError:
        await connection.close()
        msg = "Server did not respond to PING"
        raise ConnectionError(msg)
    except ConnectionError:
        raise
    except Exception as e:
        await connection.close()
        msg = f"Failed to verify connection: {e}"
        raise ConnectionError(msg)

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
        no_echo=no_echo,
        inbox_prefix=inbox_prefix,
        name=name,
        ping_interval=ping_interval,
        max_outstanding_pings=max_outstanding_pings,
        token=token,
        user=user,
        password=password,
        nkey_public_key_handler=nkey_public_key_handler,
        nkey_signature_handler=nkey_signature_handler,
        jwt_handler=jwt_handler,
        jwt_signature_handler=jwt_signature_handler,
        tls=tls,
        tls_hostname=tls_hostname,
        tls_handshake_first=tls_handshake_first,
        wants_tls=wants_tls,
        verbose=verbose,
        pedantic=pedantic,
        skip_subject_validation=skip_subject_validation,
    )

    client._status = ClientStatus.CONNECTED

    return client


__all__ = [
    "connect",
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
    "MaxPayloadError",
    "SecureConnectionRequiredError",
]
