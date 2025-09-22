"""NATS subscription implementation.

This module provides the Subscription class which represents an active
subscription to a NATS subject. Subscriptions can be used as async
iterators and context managers for ergonomic message handling.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, suppress
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import Self

if TYPE_CHECKING:
    import types

    from nats.client import Client
from nats.client.message import Message

T = TypeVar("T")


class Subscription(AsyncIterator[Message],
                   AbstractAsyncContextManager["Subscription"]):
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

    def __init__(
        self,
        subject: str,
        sid: str,
        queue_group: str,
        pending_queue: asyncio.Queue,
        client: Client,
        callback: Callable[[Message], None] | None = None,
    ):
        self._subject = subject
        self._sid = sid
        self._queue_group = queue_group
        self._client = client
        self._pending_queue = pending_queue
        self._closed = False
        self._callbacks: list[Callable[[Message], None]] = []
        if callback is not None:
            self._callbacks.append(callback)

    @property
    def sid(self) -> str:
        """Get the subscription ID."""
        return self._sid

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

    @property
    def queue(self) -> asyncio.Queue:
        """Get the message queue for this subscription."""
        return self._pending_queue

    def add_callback(self, callback: Callable[[Message], None]) -> None:
        """Add a callback to be invoked when a message is received.

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
            RuntimeError: If the subscription is closed
        """
        if self._closed:
            msg = "Subscription is closed"
            raise RuntimeError(msg)

        if timeout is not None:
            return await asyncio.wait_for(self._pending_queue.get(), timeout)

        return await self._pending_queue.get()

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

    async def close(self) -> None:
        """Close the subscription.

        This marks the subscription as draining and prevents further messages
        from being added to the queue.

        This is an alias for unsubscribe().
        """
        await self.unsubscribe()

    async def unsubscribe(self) -> None:
        """Unsubscribe from this subscription.

        This sends an UNSUB command to the server and marks the subscription as closed,
        preventing further messages from being added to the queue.
        """
        if not self._closed:
            # First unsubscribe from server
            await self._client._unsubscribe(self._sid)
            # Then mark as closed
            self._closed = True

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None,
        exc_val: BaseException | None, exc_tb: types.TracebackType | None
    ) -> None:
        """Exit the async context manager, closing the subscription."""
        await self.unsubscribe()
