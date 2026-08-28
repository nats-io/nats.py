"""NATS subscription implementation.

This module provides the Subscription class which represents an active
subscription to a NATS subject. Subscriptions can be used as async
iterators and context managers for ergonomic message handling.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, suppress
from typing import TYPE_CHECKING, Self, TypeVar

if TYPE_CHECKING:
    import types

    from nats.client import Client
from nats.client.message import Message

T = TypeVar("T")

logger = logging.getLogger(__name__)


class Subscription(AsyncIterable[Message], AbstractAsyncContextManager["Subscription"]):
    """A subscription to a NATS subject.

    This class represents an active subscription to a NATS subject.
    It can be used as an async iterator to receive messages or as an
    async context manager to automatically close the subscription when done.

    Examples:
        async for message in subscription:
            process(message)

        async with await client.subscribe("my.subject") as subscription:
            message = await subscription.next()
            process(message)
    """

    _subject: str
    _sid: str
    _queue: str
    _client: Client
    _pending_queue: asyncio.Queue[Message]
    _max_pending_messages: int | None
    _max_pending_bytes: int | None
    _pending_messages: int
    _pending_bytes: int
    _dropped_messages: int
    _dropped_bytes: int
    _callbacks: list[Callable[[Message], None]]
    _closed: bool
    _slow_consumer_reported: bool
    _max_messages: int | None
    _delivered: int

    def __init__(
        self,
        subject: str,
        sid: str,
        queue: str,
        client: Client,
        max_pending_messages: int | None = None,
        max_pending_bytes: int | None = None,
    ):
        self._subject = subject
        self._sid = sid
        self._queue = queue
        self._client = client

        maxsize = max_pending_messages if max_pending_messages is not None else 0
        self._pending_queue = asyncio.Queue(maxsize=maxsize)
        self._max_pending_messages = max_pending_messages
        self._max_pending_bytes = max_pending_bytes
        self._pending_messages = 0
        self._pending_bytes = 0
        self._dropped_messages = 0
        self._dropped_bytes = 0
        self._callbacks = []

        self._closed = False
        self._slow_consumer_reported = False
        self._max_messages = None
        self._delivered = 0

    @property
    def subject(self) -> str:
        """Get the subscription subject."""
        return self._subject

    @property
    def queue(self) -> str:
        """Get the queue group name."""
        return self._queue

    def __aiter__(self) -> AsyncIterator[Message]:
        """Return an async iterator for messages.

        This allows using the subscription as an async iterable:
            async for message in subscription:
                process(message)

        Returns:
            An async iterator that yields messages
        """
        return self.messages

    @property
    def messages(self) -> AsyncIterator[Message]:
        """Get an async iterator for messages.

        This property provides API compatibility with nats-py, allowing:
            async for message in subscription.messages:
                process(message)

        This is equivalent to iterating directly on the subscription:
            async for message in subscription:
                process(message)

        Returns:
            An async iterator that yields messages
        """

        async def iterator():
            while True:
                try:
                    yield await self.next()
                except RuntimeError:
                    break

        return iterator()

    @property
    def closed(self) -> bool:
        """Get whether the subscription is closed."""
        return self._closed

    @property
    def pending(self) -> tuple[int, int]:
        """Get the number of pending messages and bytes.

        Returns:
            Tuple of (pending_messages, pending_bytes)
        """
        return (self._pending_messages, self._pending_bytes)

    @property
    def delivered(self) -> int:
        """Get the number of messages delivered to this subscription.

        Counts messages enqueued for consumption (not those dropped due to
        slow consumer limits). Note that the :meth:`unsubscribe_after` cap is
        driven by ``delivered`` plus dropped messages — the server counts every
        message it routes — so ``delivered`` may be less than ``max_messages``
        when the subscription closes.
        """
        return self._delivered

    @property
    def max_messages(self) -> int | None:
        """Get the auto-unsubscribe cap set via :meth:`unsubscribe_after`, or None."""
        return self._max_messages

    @property
    def dropped(self) -> tuple[int, int]:
        """Get the number of dropped messages and bytes.

        Messages are dropped when the subscription cannot keep up with
        the message flow and exceeds its pending limits.

        Returns:
            Tuple of (dropped_messages, dropped_bytes)
        """
        return (self._dropped_messages, self._dropped_bytes)

    def add_callback(self, callback: Callable[[Message], None]) -> None:
        """Add a callback to be invoked when a message is received.

        Callbacks are invoked synchronously as soon as a message is received,
        before it is queued in the subscription's message queue.

        Note: Avoid performing heavy computation or blocking operations in callbacks,
        as this will block the I/O pipeline and prevent other messages from being received.

        Args:
            callback: Function to be called when a message is received
        """
        self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[Message], None]) -> None:
        """Remove a callback from the subscription.

        Args:
            callback: Function to remove from the callback list
        """
        with suppress(ValueError):
            self._callbacks.remove(callback)

    def _enqueue(self, message: Message) -> None:
        """Enqueue a message without blocking.

        This is an internal method called by the Client when dispatching messages.

        Args:
            message: The message to enqueue

        Raises:
            asyncio.QueueFull: If message count limit would be exceeded
            ValueError: If byte limit would be exceeded
        """
        message_size = len(message.data)

        if self._max_pending_bytes is not None and self._pending_bytes + message_size > self._max_pending_bytes:
            raise ValueError(f"Byte limit exceeded: {self._pending_bytes + message_size} > {self._max_pending_bytes}")

        for callback in self._callbacks:
            try:
                callback(message)
            except Exception as e:
                logger.exception("Error in message callback: %s", e)

        self._pending_queue.put_nowait(message)

        self._pending_messages += 1
        self._pending_bytes += message_size
        self._delivered += 1

        if self._max_messages is not None and self._delivered + self._dropped_messages >= self._max_messages:
            # Cap reached: stop accepting further messages locally. The server
            # counts every message it routed — including ones we dropped — so
            # dropped messages count against the cap too. The server has the
            # matching auto-unsubscribe from the UNSUB <sid> <max_messages>
            # sent by unsubscribe_after, so no extra protocol traffic is needed.
            # Drain the queue gracefully so already-enqueued messages can still
            # be consumed via next().
            self._close_local(immediate=False)

    async def next(self, timeout: float | None = None) -> Message:
        """Get the next message from the subscription.

        Args:
            timeout: How long to wait for the next message in seconds.
                    If None, wait indefinitely.

        Returns:
            The next message

        Raises:
            TimeoutError: If timeout is reached waiting for a message
            RuntimeError: If the subscription is closed and queue is empty
        """
        try:
            if timeout is not None:
                message = await asyncio.wait_for(self._pending_queue.get(), timeout)
            else:
                message = await self._pending_queue.get()

            self._pending_messages -= 1
            self._pending_bytes -= len(message.data)

            return message
        except asyncio.QueueShutDown:
            msg = "Subscription is closed"
            raise RuntimeError(msg) from None

    def _close_local(self, *, immediate: bool) -> None:
        """Close the subscription locally without sending UNSUB.

        Removes the subscription from the client's registry and shuts down the
        pending queue. With ``immediate=True`` queued messages are discarded;
        with ``immediate=False`` they remain consumable via :meth:`next`.

        Idempotent — subsequent calls are no-ops.
        """
        if self._closed:
            return

        self._client._subscriptions.pop(self._sid, None)
        self._pending_queue.shutdown(immediate=immediate)
        if immediate:
            self._pending_messages = 0
            self._pending_bytes = 0
        self._closed = True

    async def unsubscribe(self) -> None:
        """Unsubscribe from this subscription.

        This sends an UNSUB command to the server and marks the subscription as closed,
        preventing further messages from being added to the queue.
        """
        if not self._closed:
            await self._client._unsubscribe(self._sid)
            self._pending_queue.shutdown(immediate=True)
            self._pending_messages = 0
            self._pending_bytes = 0
            self._closed = True

    async def unsubscribe_after(self, max_messages: int) -> None:
        """Schedule the subscription to auto-unsubscribe after ``max_messages`` deliveries.

        Sends ``UNSUB <sid> <max_messages>`` so the server stops delivering once
        ``max_messages`` messages have been routed to this subscription, and tracks
        the same cap locally so the subscription closes itself once the routed
        count reaches it.

        The server counts every message it routes, not just the ones successfully
        enqueued — so messages dropped due to slow-consumer limits also count
        against the cap, and :attr:`delivered` may be less than ``max_messages``
        when the subscription closes.

        Returns immediately — messages keep flowing until the cap is hit. Calling
        this multiple times replaces the previous cap (the server overwrites and
        the local cap follows suit). If ``delivered`` plus dropped messages already
        meets or exceeds ``max_messages``, the subscription is closed right away.

        On reconnect the cap is re-issued as ``UNSUB <sid> remaining``, where
        ``remaining = max_messages - delivered - dropped``. If ``remaining <= 0``
        the subscription is not resubscribed.

        Args:
            max_messages: Number of messages to receive before auto-unsubscribing.
                Must be greater than zero — use :meth:`unsubscribe` for
                immediate teardown.

        Raises:
            ValueError: If ``max_messages <= 0``.
        """
        if max_messages <= 0:
            msg = "max_messages must be greater than 0; use unsubscribe() for immediate teardown"
            raise ValueError(msg)

        if self._closed:
            return

        self._max_messages = max_messages
        await self._client._unsubscribe(self._sid, max_messages=max_messages, keep=True)

        if self._delivered + self._dropped_messages >= max_messages:
            self._close_local(immediate=False)

    async def drain(self) -> None:
        """Drain the subscription.

        This unsubscribes from the server (stopping new messages), allowing all pending
        messages that are already in the queue to be processed. After drain, the
        subscription is marked as closed but pending messages can still be consumed.
        """
        if not self._closed:
            await self._client._unsubscribe(self._sid)
            self._pending_queue.shutdown(immediate=False)
            self._closed = True

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None
    ) -> None:
        """Exit the async context manager, closing the subscription."""
        await self.unsubscribe()
