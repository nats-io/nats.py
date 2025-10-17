"""NATS subscription implementation.

This module provides the Subscription class which represents an active
subscription to a NATS subject. Subscriptions can be used as async
iterators and context managers for ergonomic message handling.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, suppress
from typing import TYPE_CHECKING, Self, TypeVar

if TYPE_CHECKING:
    import types

    from nats.client import Client
from nats.client.message import Message

T = TypeVar("T")


class Subscription(AsyncIterator[Message], AbstractAsyncContextManager["Subscription"]):
    """A subscription to a NATS subject.

    This class represents an active subscription to a NATS subject.
    It can be used as an async iterator to receive messages or as an
    async context manager to automatically close the subscription when done.

    Examples:
        # As an async iterator
        async for msg in subscription:
            process(msg)

        # As a context manager
        async with await client.subscribe("my.subject") as subscription:
            msg = await subscription.next()
            process(msg)
    """

    _subject: str
    _sid: str
    _queue_group: str
    _client: Client
    _pending_queue: asyncio.Queue[Message]
    _closed: bool
    _callbacks: list[Callable[[Message], None]]

    def __init__(
        self,
        subject: str,
        sid: str,
        queue_group: str,
        pending_queue: asyncio.Queue,
        client: Client,
    ):
        self._subject = subject
        self._sid = sid
        self._queue_group = queue_group
        self._client = client
        self._pending_queue = pending_queue
        self._closed = False
        self._callbacks = []

    @property
    def subject(self) -> str:
        """Get the subscription subject."""
        return self._subject

    @property
    def queue_group(self) -> str:
        """Get the queue group name."""
        return self._queue_group

    @property
    def closed(self) -> bool:
        """Get whether the subscription is closed."""
        return self._closed

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

    async def next(self, timeout: float | None = None) -> Message:
        """Get the next message from the subscription.

        Args:
            timeout: How long to wait for the next message in seconds.
                    If None, wait indefinitely.

        Returns:
            The next message

        Raises:
            asyncio.TimeoutError: If timeout is reached waiting for a message
            RuntimeError: If the subscription is closed and queue is empty
        """
        try:
            if timeout is not None:
                return await asyncio.wait_for(self._pending_queue.get(), timeout)

            return await self._pending_queue.get()
        except asyncio.QueueShutDown:
            msg = "Subscription is closed"
            raise RuntimeError(msg) from None

    async def __anext__(self) -> Message:
        """Get the next message from the subscription.

        This allows using the subscription as an async iterator:
            async for msg in subscription:
                ...
        """
        try:
            return await self.next()
        except RuntimeError:
            raise StopAsyncIteration from None

    async def unsubscribe(self) -> None:
        """Unsubscribe from this subscription.

        This sends an UNSUB command to the server and marks the subscription as closed,
        preventing further messages from being added to the queue.
        """
        if not self._closed:
            # Send UNSUB to server and remove from client's subscription map
            await self._client._unsubscribe(self._sid)
            # Shutdown queue immediately (discard pending messages)
            self._pending_queue.shutdown(immediate=True)
            # Mark as closed
            self._closed = True

    async def drain(self) -> None:
        """Drain the subscription.

        This unsubscribes from the server (stopping new messages), allowing all pending
        messages that are already in the queue to be processed. After drain, the
        subscription is marked as closed but pending messages can still be consumed.
        """
        if not self._closed:
            # Send UNSUB to server to stop new messages
            await self._client._unsubscribe(self._sid)
            # Shutdown queue gracefully (allow pending messages to be consumed)
            self._pending_queue.shutdown(immediate=False)
            # Keep in client's subscription list until queue is drained
            # Mark as closed
            self._closed = True

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None
    ) -> None:
        """Exit the async context manager, closing the subscription."""
        await self.unsubscribe()
