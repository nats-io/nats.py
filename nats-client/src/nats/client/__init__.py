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
    from importlib.metadata import version
    __version__ = version("nats-client")
except Exception:
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
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from nats.client.connection import Connection, open_tcp_connection
from nats.client.errors import BadRequestError, NoRespondersError, StatusError
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
    ServerInfo as ProtocolServerInfo,
)
from nats.client.subscription import Subscription
from typing_extensions import Self

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
        )


def _collect_servers(server_info: ServerInfo,
                     *,
                     no_randomize: bool = False) -> list[str]:
    """Collect servers from server info.

    Args:
        server_info: Server information
        no_randomize: Whether to disable randomizing the server pool

    Returns:
        List of server addresses
    """
    # Start with current server
    servers = [f"{server_info.host}:{server_info.port}"]

    # Add discovered servers
    if server_info.connect_urls:
        servers.extend(server_info.connect_urls)

    # Shuffle the pool unless no_randomize is set
    if not no_randomize:
        random.shuffle(servers)

    return servers


class Client(AbstractAsyncContextManager["Client"]):
    """High-level NATS client."""

    def __init__(
        self,
        connection: Connection,
        server_info: ServerInfo,
        *,
        allow_reconnect: bool = True,
        reconnect_attempts: int = 10,
        reconnect_time_wait: float = 2.0,
        reconnect_time_wait_max: float = 10.0,
        reconnect_jitter: float = 0.1,
        reconnect_timeout: float = 2.0,
        no_randomize: bool = False,
    ):
        """Initialize the client.

        Args:
            connection: NATS connection
            server_info: Server information
            allow_reconnect: Whether to automatically reconnect if the connection is lost
            reconnect_attempts: Maximum number of reconnection attempts (0 for unlimited)
            reconnect_time_wait: Initial wait time between reconnection attempts
            reconnect_time_wait_max: Maximum wait time between reconnection attempts
            reconnect_jitter: Jitter factor for reconnection attempts
            reconnect_timeout: Timeout for reconnection attempts
            no_randomize: Whether to disable randomizing the server pool
        """
        self._connection = connection
        self._server_info = server_info
        self._allow_reconnect = allow_reconnect
        self._reconnect_attempts = reconnect_attempts
        self._reconnect_time_wait = reconnect_time_wait
        self._reconnect_time_wait_max = reconnect_time_wait_max
        self._reconnect_jitter = reconnect_jitter
        self._reconnect_timeout = reconnect_timeout
        self._no_randomize = no_randomize
        self._status = ClientStatus.CONNECTING
        self._subscriptions: dict[str, Subscription] = {}
        self._next_sid = 1
        self._last_error: str | None = None

        # Server pool management
        self._server_pool = _collect_servers(
            server_info, no_randomize=no_randomize
        )

        # Reconnection state
        self._reconnect_attempts_counter = 0
        self._reconnecting = False
        self._reconnect_time = self._reconnect_time_wait
        self._last_server: str | None = None

        # Subscriptions
        self._pending_bytes: int = 0  # Current bytes pending to be written
        self._pending_messages: list[bytes] = [
        ]  # Current messages pending to be written
        self._max_pending_bytes: int = 1 * 1024 * 1024  # 1mb max pending bytes
        self._max_pending_messages: int = 1 * 512  # Max pending messages before flush
        self._min_flush_interval: float = 0.005  # 5ms minimum between flushes
        self._last_flush: float = (
            asyncio.get_event_loop().time() - self._min_flush_interval
        )  # Initialize to allow immediate flush
        self._flush_waker: asyncio.Event = asyncio.Event(
        )  # Wakes up write loop when data needs to be flushed

        # Ping/Pong keep-alive
        self._ping_interval: float = 120.0  # 2 minutes
        self._max_outstanding_pings: int = 2
        self._pings_outstanding: int = 0
        self._last_pong_received: float = asyncio.get_event_loop().time()
        self._last_ping_sent: float = self._last_pong_received
        self._pong_waker: asyncio.Event = asyncio.Event(
        )  # Wakes up code waiting for PONG

        # Callbacks
        self._disconnected_callbacks: list[Callable[[], None]] = []
        self._reconnected_callbacks: list[Callable[[], None]] = []
        self._error_callbacks: list[Callable[[str], None]] = []

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

    async def _read_loop(self) -> None:
        """Background task that reads and processes incoming protocol messages."""
        try:
            while True:
                try:
                    msg = await parse(self._connection)

                    if not msg:
                        logger.info("Connection closed by server")
                        break

                    # Handle message based on type
                    match msg:
                        case ("MSG", subject, sid, reply_to, payload):
                            logger.debug(
                                "<<- MSG %s %s %s %s", subject, sid,
                                reply_to if reply_to else "", len(payload)
                            )
                            await self._handle_msg(
                                subject, sid, reply_to, payload
                            )
                        case (
                            "HMSG", subject, sid, reply_to, headers, payload,
                            status_code, status_description
                        ):
                            logger.debug(
                                "<<- HMSG %s %s %s %s %s", subject, sid,
                                reply_to, len(headers), len(payload)
                            )
                            await self._handle_hmsg(
                                subject, sid, reply_to, headers, payload,
                                status_code, status_description
                            )
                        case ("PING", ):
                            logger.debug("<<- PING")
                            await self._handle_ping()
                        case ("PONG", ):
                            logger.debug("<<- PONG")
                            await self._handle_pong()
                        case ("INFO", info):
                            logger.debug(
                                "<<- INFO %s...",
                                json.dumps(info)[:80]
                            )
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

        # Connection lost, initiate disconnect/reconnect process
        # No need to check status here as _force_disconnect will handle that
        await self._force_disconnect()

    async def _handle_ping(self) -> None:
        """Handle PING from server."""
        logger.debug("->> PONG")
        await self._connection.write(encode_pong())

    async def _handle_pong(self) -> None:
        """Handle PONG from server."""
        self._last_pong_received = asyncio.get_event_loop().time()
        self._pings_outstanding = 0
        self._pong_waker.set()  # Wake up code waiting for PONG

    async def _queue_ping(self) -> bool:
        """Queue a PING to be sent after the next flush.

        Returns:
            bool: True if a PING was queued, False if max outstanding PINGs reached.
        """
        if self._pings_outstanding >= self._max_outstanding_pings:
            logger.error("Max outstanding PINGs reached")
            await self._force_disconnect()
            return False

        # Mark that we should send a PING after flush
        self._pings_outstanding += 1
        self._last_ping_sent = asyncio.get_event_loop().time()
        await self._connection.write(encode_ping())
        return True

    async def _write_loop(self) -> None:
        """Background task that handles periodic flushes and PINGs."""
        try:
            while self._status == ClientStatus.CONNECTED:
                try:
                    # Wait for either a flush request or PING interval
                    try:
                        # No pending messages, wait for flush request or ping interval
                        await asyncio.wait_for(
                            self._flush_waker.wait(),
                            timeout=self._ping_interval
                        )
                        self._flush_waker.clear()

                        # If we got here, a flush was requested
                        current_time = asyncio.get_event_loop().time()
                        since_last_flush = current_time - self._last_flush
                        if since_last_flush < self._min_flush_interval:
                            await asyncio.sleep(
                                self._min_flush_interval - since_last_flush
                            )

                        # Perform the flush if we have messages
                        if self._pending_messages:
                            await self._force_flush()
                            self._last_flush = current_time

                    except asyncio.TimeoutError:
                        # PING interval elapsed without flush requests
                        current_time = asyncio.get_event_loop().time()

                        # Check if we need to send a PING
                        if current_time - self._last_ping_sent >= self._ping_interval:
                            if self._pings_outstanding >= self._max_outstanding_pings:
                                logger.exception(
                                    "Max outstanding PINGs reached"
                                )
                                await self._force_disconnect()
                                break

                            # Flush any pending messages before PING
                            if self._pending_messages:
                                await self._force_flush()
                                self._last_flush = current_time

                            # Send PING without waiting for PONG
                            await self._queue_ping()

                except Exception:
                    logger.exception("Error in write loop")
                    if self._status != ClientStatus.CONNECTED:
                        break
                    # Don't break the loop for non-fatal errors while connected

        except asyncio.CancelledError:
            # Final flush on cancellation
            if self._pending_messages:
                try:
                    await self._force_flush()
                except Exception:
                    logger.exception("Error during final flush")
            return

        # No catch-all disconnect handler here - the read loop will handle disconnection

    async def _handle_msg(
        self, subject: str, sid: str, reply_to: str | None, payload: bytes
    ) -> None:
        """Handle MSG from server."""
        if sid in self._subscriptions:
            subscription = self._subscriptions[sid]
            msg = Message(subject=subject, data=payload, reply_to=reply_to)

            # Invoke callbacks if available
            for callback in subscription._callbacks:
                try:
                    callback(msg)
                except Exception:
                    logger.exception("Error in subscription callback")

            try:
                await subscription.queue.put(msg)
            except Exception:
                logger.exception("Error putting message in queue")

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
        if sid in self._subscriptions:
            subscription = self._subscriptions[sid]
            # Create Status object if status information is present
            status = None
            if status_code is not None:
                status = Status(
                    code=status_code, description=status_description
                )

            msg = Message(
                subject=subject,
                data=payload,
                reply_to=reply_to,
                headers=Headers(headers) if headers else None,
                status=status,
            )

            # Invoke callbacks if available
            for callback in subscription._callbacks:
                try:
                    callback(msg)
                except Exception:
                    logger.exception("Error in subscription callback")

            try:
                await subscription.queue.put(msg)
            except Exception:
                logger.exception("Error putting message in queue")

    async def _handle_info(self, info: dict) -> None:
        """Handle INFO from server."""
        self._server_info = ServerInfo.from_protocol(info)
        self._server_pool = _collect_servers(
            self._server_info, no_randomize=self._no_randomize
        )

    async def _handle_error(self, error: str) -> None:
        """Handle ERR from server."""
        self._last_error = error

        # Call error callback if set
        if self._error_callbacks:
            for callback in self._error_callbacks:
                try:
                    callback(error)
                except Exception:
                    logger.exception("Error in error callback")

    async def _force_disconnect(self) -> None:
        """Force disconnect from server."""
        logger.info("Force disconnecting")

        # First, disconnect - this part remains unchanged
        old_status = self._status
        self._status = ClientStatus.CLOSED

        # Cancel and cleanup existing tasks immediately
        if self._read_task and isinstance(
                self._read_task, asyncio.Task) and not self._read_task.done():
            self._read_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._read_task

        if self._write_task and isinstance(
                self._write_task,
                asyncio.Task) and not self._write_task.done():
            self._write_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._write_task

        await self._connection.close()

        # Only attempt to reconnect if:
        # 1. We were not explicitly closing
        # 2. Reconnect is enabled
        # 3. We're not already reconnecting
        if (old_status not in (ClientStatus.CLOSING, ClientStatus.CLOSED)
                and self._allow_reconnect and not self._reconnecting):
            logger.info("Starting reconnection process")
            self._status = ClientStatus.RECONNECTING

            # Call disconnected callback
            if self._disconnected_callbacks:
                for callback in self._disconnected_callbacks:
                    try:
                        callback()
                    except Exception:
                        logger.exception("Error in disconnected callback")

            # Start reconnection process
            self._reconnecting = True
            self._reconnect_attempts_counter = 0
            self._reconnect_time = self._reconnect_time_wait

            while self._reconnect_attempts == 0 or self._reconnect_attempts_counter < self._reconnect_attempts:
                # Check if reconnection has been disabled during reconnection attempts
                if not self._allow_reconnect:
                    logger.info(
                        "Reconnection aborted - allow_reconnect flag disabled"
                    )
                    break

                self._reconnect_attempts_counter += 1
                logger.info(
                    "Reconnection attempt %s", self._reconnect_attempts_counter
                )

                try:
                    # Apply jitter to wait time
                    actual_wait = self._reconnect_time * (
                        1 + random.random() * self._reconnect_jitter
                    )

                    logger.info(
                        "Waiting %.2fs before reconnection attempt",
                        actual_wait
                    )
                    await asyncio.sleep(actual_wait)

                    # Try each server in the pool
                    for server in self._server_pool:
                        if server == self._last_server and len(
                                self._server_pool) > 1:
                            continue

                        logger.info("Trying to reconnect to %s", server)

                        # Parse server address
                        if "://" in server:
                            parsed_url = urlparse(server)
                        else:
                            # Server addresses from connect_urls don't have scheme
                            # Prepend the appropriate scheme and parse
                            scheme = "tls" if self._server_info.tls_required else "nats"

                            # If address has brackets already or no colons, use as-is
                            # Otherwise check if it's IPv6 (multiple colons) and needs brackets
                            if not server.startswith("[") and server.count(":") > 1:
                                # IPv6 address without brackets - need to add them
                                # Split on last colon to separate host from port
                                last_colon = server.rfind(":")
                                try:
                                    # Try to parse as port
                                    port_val = int(server[last_colon + 1:])
                                    if 0 <= port_val <= 65535:
                                        # Valid port, wrap host in brackets
                                        host_part = server[:last_colon]
                                        server = f"[{host_part}]:{port_val}"
                                except ValueError:
                                    # Not a valid port, treat whole thing as IPv6 host
                                    server = f"[{server}]"

                            parsed_url = urlparse(f"{scheme}://{server}")

                        host = parsed_url.hostname
                        port = parsed_url.port or 4222
                        scheme = parsed_url.scheme

                        try:
                            # Open new connection based on server info
                            if scheme in ("tls", "wss"):
                                ssl_context = ssl.create_default_context()
                                connection = await asyncio.wait_for(
                                    open_tcp_connection(
                                        host, port, ssl_context=ssl_context
                                    ),
                                    timeout=self._reconnect_timeout,
                                )
                            else:
                                connection = await asyncio.wait_for(
                                    open_tcp_connection(host, port),
                                    timeout=self._reconnect_timeout,
                                )

                            # Read INFO message
                            msg = await parse(connection)
                            if not msg or msg.op != "INFO":
                                msg = "Expected INFO message"
                                raise RuntimeError(msg)

                            new_server_info = ServerInfo.from_protocol(msg.info)
                            logger.info(
                                "Reconnected to %s (version %s)",
                                new_server_info.server_id,
                                new_server_info.version
                            )

                            # Send CONNECT
                            connect_info = ConnectInfo(
                                verbose=False,
                                pedantic=False,
                                lang="python",
                                version=__version__,
                                protocol=1,
                                headers=True,
                            )
                            logger.debug(
                                "->> CONNECT %s", json.dumps(connect_info)
                            )
                            await connection.write(
                                encode_connect(connect_info)
                            )

                            # Update client state with new connection
                            self._connection = connection
                            self._server_info = new_server_info
                            self._status = ClientStatus.CONNECTED
                            self._last_server = server

                            # Update server pool with new discovered servers
                            self._server_pool = _collect_servers(
                                new_server_info,
                                no_randomize=self._no_randomize
                            )

                            # Resubscribe to all active subscriptions
                            for sid, subscription in list(
                                    self._subscriptions.items()):
                                subject = subscription.subject
                                queue_group = subscription.queue_group
                                logger.debug(
                                    "->> SUB %s %s %s", subject, sid,
                                    queue_group
                                )
                                await self._connection.write(
                                    encode_sub(subject, sid, queue_group)
                                )

                            # Flush to ensure all resubscriptions are sent
                            await self._force_flush()

                            # Cancel existing tasks, if they are running
                            # Tasks were already canceled and cleaned up at the start of _force_disconnect
                            # Just create new ones
                            self._read_task = asyncio.create_task(
                                self._read_loop()
                            )
                            self._write_task = asyncio.create_task(
                                self._write_loop()
                            )

                            # Reset reconnection state
                            self._reconnecting = False
                            self._reconnect_attempts_counter = 0
                            self._reconnect_time = self._reconnect_time_wait

                            # Call reconnected callback
                            if self._reconnected_callbacks:
                                for callback in self._reconnected_callbacks:
                                    try:
                                        callback()
                                    except Exception:
                                        logger.exception(
                                            "Error in reconnected callback"
                                        )

                            return  # Successfully reconnected

                        except Exception:
                            logger.exception("Failed to connect to %s", server)
                            self._last_server = server
                            continue  # Try next server

                    # If we get here, we've tried all servers in the pool
                    logger.error("Failed to connect to any server in the pool")

                    # Increase wait time for next attempt (up to max)
                    self._reconnect_time = min(
                        self._reconnect_time * 2, self._reconnect_time_wait_max
                    )

                except Exception:
                    logger.exception("Reconnection attempt failed")

            # Reconnection failed after max attempts
            logger.error("Reconnection failed after maximum attempts")
            self._reconnecting = False
            self._status = ClientStatus.CLOSED

    async def _force_flush(self) -> None:
        """Flush pending messages to the server."""
        if not self._pending_messages:
            return

        # Write all pending messages in a single operation
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

        # Flush messages
        await self._force_flush()

        # Send PING and wait for PONG
        self._pong_waker.clear()  # Clear any previous PONG wakeup
        logger.debug("->> PING")
        self._pings_outstanding += 1
        self._last_ping_sent = asyncio.get_event_loop().time()
        await self._connection.write(encode_ping())

        # Wait for PONG with timeout
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
        if self._status == ClientStatus.CLOSED:
            msg = "Connection is closed"
            raise RuntimeError(msg)

        # Get encoded command parts
        if headers:
            headers_dict = headers._headers if isinstance(
                headers, Headers
            ) else headers
            command_parts = encode_hpub(
                subject,
                payload,
                reply_to=reply_to,
                headers=headers_dict,
            )
        else:
            command_parts = encode_pub(
                subject,
                payload,
                reply_to=reply_to,
            )

        # Calculate total message size and join parts
        message_data = b"".join(command_parts)
        message_size = len(message_data)

        # Check if adding this message would exceed limits
        if (self._pending_bytes + message_size > self._max_pending_bytes
                or len(self._pending_messages) >= self._max_pending_messages):
            await self._force_flush()

        # Add message to pending batch
        self._pending_messages.append(message_data)
        self._pending_bytes += message_size

        # Wake up write loop to handle pending messages
        self._flush_waker.set()

    async def subscribe(
        self,
        subject: str,
        *,
        queue_group: str = "",
        callback: Callable[[Message], None] | None = None,
    ) -> Subscription:
        """Subscribe to a subject."""
        if self._status == ClientStatus.CLOSED:
            msg = "Connection is closed"
            raise RuntimeError(msg)

        # Create subscription
        sid = str(self._next_sid)
        self._next_sid += 1

        # Create message queue and subscription
        message_queue = asyncio.Queue()

        # Create the subscription
        subscription = Subscription(
            subject,
            sid,
            queue_group,
            message_queue,
            self,
            callback=callback,
        )

        # Store the subscription in our map
        self._subscriptions[sid] = subscription

        # Send SUB command to server
        command = encode_sub(subject, sid, queue_group)
        if queue_group:
            logger.debug("->> SUB %s %s %s", subject, queue_group, sid)
        else:
            logger.debug("->> SUB %s %s", subject, sid)

        await self._connection.write(command)

        return subscription

    async def _subscribe(
        self, subject: str, sid: str, queue_group: str | None
    ) -> asyncio.Queue:
        """Create a subscription on the server and return the message queue.

        This method is deprecated and maintained for backward compatibility.
        Use subscribe() instead.

        Args:
            subject: The subject to subscribe to
            sid: The subscription ID
            queue_group: Optional queue group for load balancing

        Returns:
            An asyncio.Queue that will receive messages for this subscription
        """
        # Create queue
        queue = asyncio.Queue()

        # Send SUB command with queue group if provided
        command = encode_sub(subject, sid, queue_group)
        if queue_group:
            logger.debug("->> SUB %s %s %s", subject, queue_group, sid)
        else:
            logger.debug("->> SUB %s %s", subject, sid)

        await self._connection.write(command)

        return queue

    async def _unsubscribe(self, sid: str) -> None:
        """Unsubscribe from a subject."""
        logger.debug("->> UNSUB %s", sid)

        if sid in self._subscriptions:
            try:
                # Send unsub to server if still connected
                if self._status not in (ClientStatus.CLOSED,
                                        ClientStatus.CLOSING):
                    await self._connection.write(encode_unsub(sid))

                # Signal queue that subscription is closed
                await self._subscriptions[sid].queue.put(None)
            except Exception:
                logger.exception("Error during unsubscribe")
            finally:
                # Always remove from our tracking
                del self._subscriptions[sid]

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

        # Create inbox for response
        inbox = f"_INBOX.{uuid.uuid4().hex}"
        logger.debug("Created inbox %s for request to %s", inbox, subject)

        # Subscribe to inbox
        sub = await self.subscribe(inbox)
        try:
            # Publish request
            await self.publish(
                subject, payload, reply_to=inbox, headers=headers
            )

            # Wait for response
            try:
                response = await asyncio.wait_for(sub.next(), timeout)

                # Check for status errors if return_on_error is False
                if not return_on_error and response.is_error_status:
                    status = response.status.code
                    description = response.status.description or "Unknown error"
                    raise StatusError.from_status(
                        status, description, subject=subject
                    )

                return response
            except asyncio.TimeoutError:
                logger.exception(
                    "Request timeout (%ss) on %s", timeout, subject
                )
                msg = "Request timeout"
                raise TimeoutError(msg)

        finally:
            await self._unsubscribe(sub.sid)

    async def close(self) -> None:
        """Close the connection."""
        if self._status == ClientStatus.CLOSED:
            return

        logger.info("Closing connection")
        self._status = ClientStatus.CLOSING

        # Disable reconnect
        self._allow_reconnect = False

        # Cancel and cleanup tasks
        if self._read_task and isinstance(
                self._read_task, asyncio.Task) and not self._read_task.done():
            self._read_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._read_task

        if self._write_task and isinstance(
                self._write_task,
                asyncio.Task) and not self._write_task.done():
            self._write_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._write_task

        # Close all subscriptions first to prevent new messages
        subscription_count = len(self._subscriptions)
        if subscription_count > 0:
            logger.debug("Closing %s subscriptions", subscription_count)

            # Make a copy of subscriptions keys since we'll be removing items while iterating
            sids = list(self._subscriptions.keys())
            for sid in sids:
                if sid in self._subscriptions:  # Check again as it might have been removed
                    subscription = self._subscriptions[sid]
                    # Close the subscription
                    await subscription.unsubscribe()

        # Close connection
        try:
            await self._connection.close()
        except Exception:
            logger.exception(
                "Error closing connection during force disconnect"
            )

        # Wake up write loop before cancelling
        self._flush_waker.set()

        # Cancel and clean up tasks
        tasks_to_cancel = []
        if self._read_task and not self._read_task.done():
            tasks_to_cancel.append(self._read_task)
            self._read_task.cancel()

        if self._write_task and not self._write_task.done():
            tasks_to_cancel.append(self._write_task)
            self._write_task.cancel()

        # Wait for all tasks to complete
        if tasks_to_cancel:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        self._status = ClientStatus.CLOSED

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None,
        exc_val: BaseException | None, exc_tb: types.TracebackType | None
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
            lang="python",
            version=__version__,
            protocol=1,
            headers=True,
        )
        logger.debug("->> CONNECT %s", json.dumps(connect_info))
        await self._connection.write(encode_connect(connect_info))
        self._status = ClientStatus.CONNECTED


async def connect(
    url: str = "nats://localhost:4222",
    *,
    timeout: float = 2.0,
    allow_reconnect: bool = True,
    reconnect_attempts: int = 10,
    reconnect_time_wait: float = 2.0,
    reconnect_time_wait_max: float = 10.0,
    reconnect_jitter: float = 0.1,
) -> Client:
    """Connect to a NATS server.

    Args:
        url: Server URL
        timeout: Connection timeout in seconds
        allow_reconnect: Whether to automatically reconnect if the connection is lost
        reconnect_attempts: Maximum number of reconnection attempts (0 for unlimited)
        reconnect_time_wait: Initial wait time between reconnection attempts
        reconnect_time_wait_max: Maximum wait time between reconnection attempts
        reconnect_jitter: Jitter factor for reconnection attempts

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

    # Get host and port
    host = parsed_url.hostname or "localhost"
    port = parsed_url.port or 4222

    logger.info("Connecting to %s:%s", host, port)

    try:
        # Open connection with timeout
        match parsed_url.scheme:
            case "tls":
                ssl_context = ssl.create_default_context()
                connection = await asyncio.wait_for(
                    open_tcp_connection(host, port, ssl_context=ssl_context),
                    timeout=timeout,
                )
            case "nats":
                connection = await asyncio.wait_for(
                    open_tcp_connection(host, port),
                    timeout=timeout,
                )
            case _:
                msg = f"Unsupported scheme: {parsed_url.scheme}"
                raise ValueError(msg)

        try:
            # Read INFO message
            msg = await parse(connection)
            if not msg or msg.op != "INFO":
                msg = "Expected INFO message"
                raise RuntimeError(msg)

            # Parse server info
            server_info = ServerInfo.from_protocol(msg.info)
            logger.info(
                "Connected to %s (version %s)", server_info.server_id,
                server_info.version
            )

            # Create client
            client = Client(
                connection,
                server_info,
                allow_reconnect=allow_reconnect,
                reconnect_attempts=reconnect_attempts,
                reconnect_time_wait=reconnect_time_wait,
                reconnect_time_wait_max=reconnect_time_wait_max,
                reconnect_jitter=reconnect_jitter,
            )

            # Send CONNECT message
            connect_info = ConnectInfo(
                verbose=False,
                pedantic=False,
                lang="python",
                version=__version__,
                protocol=1,
                headers=True,
                no_responders=True,
            )
            logger.debug("->> CONNECT %s", json.dumps(connect_info))
            await connection.write(encode_connect(connect_info))
            client._status = ClientStatus.CONNECTED

            return client

        except Exception as e:
            await connection.close()
            msg = f"Failed to connect: {e}"
            raise ConnectionError(msg)

    except asyncio.TimeoutError:
        msg = f"Connection timed out after {timeout} seconds"
        raise TimeoutError(msg)
    except Exception as e:
        msg = f"Failed to connect: {e}"
        raise ConnectionError(msg)


__all__ = [
    "__version__",
    "Message",
    "Headers",
    "Status",
    "Subscription",
    "Client",
    "ServerInfo",
    "ClientStatus",
    "StatusError",
    "BadRequestError",
    "NoRespondersError",
]
