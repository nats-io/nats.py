from __future__ import annotations

import asyncio
import json
import time
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    overload,
)

from nats.jetstream.consumer import (
    Consumer,
    ConsumerInfo,
    MessageBatch,
    MessageStream,
)
from nats.jetstream.message import Message
from nats.jetstream.util import new_inbox

if TYPE_CHECKING:
    from nats.client import Subscription
    from nats.client.message import Message as ClientMessage
    from nats.jetstream.stream import Stream


class PullMessageBatch(MessageBatch):
    _subscription: Subscription
    _pending_messages: int
    _jetstream: Any
    _terminated: bool
    _error: Exception | None
    _deadline: float | None

    def __init__(self, subscription: Subscription, batch_size: int, jetstream, max_wait: float | None):
        self._subscription = subscription
        self._pending_messages = batch_size
        self._jetstream = jetstream
        self._terminated = False
        self._error = None
        self._deadline = time.time() + max_wait if max_wait is not None else None

    @property
    def error(self) -> Exception | None:
        return self._error

    def __aiter__(self) -> AsyncIterator[Message]:
        return self

    async def __anext__(self) -> Message:
        if self._terminated or self._pending_messages <= 0:
            if not self._terminated:
                await self._subscription.unsubscribe()
                self._terminated = True
            raise StopAsyncIteration

        try:
            while True:
                # Calculate remaining timeout
                timeout = None
                if self._deadline is not None:
                    timeout = max(0, self._deadline - time.time())
                    if timeout <= 0:
                        raise StopAsyncIteration

                try:
                    raw_msg: ClientMessage = await self._subscription.next(timeout=timeout)
                except (TimeoutError, asyncio.TimeoutError):
                    # If no more messages after timeout, we're done
                    raise StopAsyncIteration

                # Check for status messages
                if raw_msg.status is not None:
                    match raw_msg.status.code:
                        case "100":  # Heartbeat
                            continue
                        case "404":  # No messages
                            raise StopAsyncIteration
                        case "408":  # Timeout
                            raise StopAsyncIteration
                        case "409":  # Message Size Exceeds MaxBytes
                            raise StopAsyncIteration
                        case _:
                            self._error = Exception(f"Status {raw_msg.status.code}: {raw_msg.status.description or ''}")
                            raise StopAsyncIteration

                # Convert to JetStream message
                js_msg = Message(
                    data=raw_msg.data,
                    subject=raw_msg.subject,
                    reply_to=raw_msg.reply_to,
                    headers=raw_msg.headers,
                    jetstream=self._jetstream,
                )

                self._pending_messages -= 1
                return js_msg
        except (StopAsyncIteration, asyncio.TimeoutError):
            if not self._terminated:
                await self._subscription.unsubscribe()
                self._terminated = True
            raise StopAsyncIteration


class PullMessageStream(MessageStream):
    """Continuous message stream for pull consumers, similar to Rust's Stream."""

    _consumer: PullConsumer
    _subscription: Subscription
    _batch: int
    _max_bytes: int | None
    _heartbeat: float | None
    _expires: float
    _terminated: bool
    _pending_messages: int
    _pending_bytes: int
    _request_task: asyncio.Task[None] | None
    _started: bool

    def __init__(
        self,
        consumer: PullConsumer,
        subscription: Subscription,
        batch: int = 100,
        max_bytes: int | None = None,
        heartbeat: float | None = None,
        expires: float = 30.0,
    ):
        self._consumer = consumer
        self._subscription = subscription
        self._batch = batch
        self._max_bytes = max_bytes
        self._heartbeat = heartbeat
        self._expires = expires
        self._terminated = False
        self._pending_messages = 0
        self._pending_bytes = 0
        self._request_task: asyncio.Task | None = None
        self._started = False

    @property
    def is_active(self) -> bool:
        """Check if the message stream is still active."""
        return not self._terminated

    def __aiter__(self) -> AsyncIterator[Message]:
        return self

    async def __anext__(self) -> Message:
        if self._terminated:
            raise StopAsyncIteration

        # Start the request loop task on first access
        if not self._started:
            self._started = True
            self._request_task = asyncio.create_task(self._request_loop())

        # Get next message from subscription
        while True:
            try:
                raw_msg: ClientMessage = await self._subscription.next()
            except Exception:
                await self._cleanup()
                raise StopAsyncIteration

            # Handle status messages
            if raw_msg.status is not None:
                match raw_msg.status.code:
                    case "100":  # Heartbeat
                        continue
                    case "404":  # No messages - batch is complete
                        # Reset pending counts as the batch is done
                        self._pending_messages = 0
                        self._pending_bytes = 0
                        # Immediately request more messages
                        await self._send_request()
                        continue
                    case "408":  # Timeout
                        continue
                    case "409":  # Message Size Exceeds MaxBytes - batch is complete due to size limit
                        # Reset pending counts as the batch is done
                        self._pending_messages = 0
                        self._pending_bytes = 0
                        # Immediately request more messages
                        await self._send_request()
                        continue
                    case _:  # Error
                        await self._cleanup()
                        raise StopAsyncIteration

            # Track that we consumed a message
            self._pending_messages = max(0, self._pending_messages - 1)
            self._pending_bytes = max(0, self._pending_bytes - len(raw_msg.data))

            # Convert to JetStream message
            return Message(
                data=raw_msg.data,
                subject=raw_msg.subject,
                reply_to=raw_msg.reply_to,
                headers=raw_msg.headers,
                jetstream=self._consumer._stream._jetstream,
            )

    async def _send_request(self):
        """Send a pull request to the server."""
        if self._terminated:
            return

        jetstream = self._consumer._stream._jetstream

        request: dict[str, Any] = {
            "batch": self._batch,
            "expires": int(self._expires * 1_000_000_000),
            "no_wait": False,
        }

        if self._heartbeat is not None:
            request["idle_heartbeat"] = int(self._heartbeat * 1_000_000_000)

        if self._max_bytes is not None:
            request["max_bytes"] = self._max_bytes

        api_prefix = jetstream.api_prefix
        subject = f"{api_prefix}.CONSUMER.MSG.NEXT.{self._consumer.stream_name}.{self._consumer.name}"
        payload = json.dumps(request).encode()

        await jetstream.client.publish(subject, payload=payload, reply_to=self._subscription.subject)

        self._pending_messages += self._batch
        if self._max_bytes is not None:
            self._pending_bytes += self._max_bytes

    async def _request_loop(self):
        """Background task that continuously ensures we have pending requests."""
        try:
            # Send initial request
            await self._send_request()

            while not self._terminated:
                await asyncio.sleep(0.1)  # Check every 100ms

                # Send new request if we're running low on messages
                should_request = self._pending_messages < self._batch // 2

                # Also consider bytes if we have a limit
                if self._max_bytes is not None:
                    should_request = should_request or self._pending_bytes < self._max_bytes // 2

                if should_request:
                    await self._send_request()
        except asyncio.CancelledError:
            pass
        except Exception:
            # If request loop fails, terminate the stream
            self._terminated = True

    async def stop(self):
        """Stop the message stream and clean up resources."""
        await self._cleanup()

    async def _cleanup(self):
        """Clean up resources."""
        self._terminated = True
        if self._request_task:
            self._request_task.cancel()
            try:
                await self._request_task
            except asyncio.CancelledError:
                pass
            self._request_task = None

        await self._subscription.unsubscribe()


class PullConsumer(Consumer):
    _stream: Stream
    _info: ConsumerInfo

    def __init__(self, stream: Stream, info: ConsumerInfo):
        self._stream = stream
        self._info = info

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def stream_name(self) -> str:
        return self._info.stream_name

    @property
    def info(self) -> ConsumerInfo:
        return self._info

    async def get_info(self) -> ConsumerInfo:
        # Refresh info from server
        return self._info

    async def next(
        self,
        max_wait: float = 5.0,
        heartbeat: float | None = None,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
    ) -> Message:
        """Fetch the next single message from the consumer.

        Args:
            max_wait: Maximum time to wait for a message in seconds
            heartbeat: Heartbeat interval in seconds
            min_ack_pending: Minimum number of unacknowledged messages that can be pending
            min_pending: Minimum number of messages that should be pending in the stream
            priority_group: Priority group for message fetching

        Returns:
            The next message

        Raises:
            asyncio.TimeoutError: If no message is received within the timeout
        """
        batch = await self.fetch(
            max_messages=1,
            max_wait=max_wait,
            heartbeat=heartbeat,
            min_ack_pending=min_ack_pending,
            min_pending=min_pending,
            priority_group=priority_group,
        )
        async for msg in batch:
            return msg
        raise asyncio.TimeoutError("No message received within timeout")

    async def messages(
        self,
        *,
        max_messages: int | None = None,
        max_bytes: int | None = None,
        heartbeat: float | None = None,
        max_wait: float = 30.0,
    ) -> MessageStream:
        """Create a continuous message stream for manual iteration.

        Args:
            max_messages: Maximum number of messages to request per batch
            max_bytes: Maximum bytes per batch (can be used together with max_messages)
            heartbeat: Heartbeat interval in seconds
            max_wait: Request expiration time in seconds

        Returns:
            MessageStream for manual message consumption
        """

        jetstream = self._stream._jetstream
        inbox = new_inbox()
        subscription = await jetstream.client.subscribe(inbox)

        stream = PullMessageStream(
            consumer=self,
            subscription=subscription,
            batch=max_messages or 100,  # Default to 100 if max_messages is None
            max_bytes=max_bytes,
            heartbeat=heartbeat,
            expires=max_wait,
        )

        return stream

    @overload
    async def fetch(
        self,
        *,
        max_messages: int,
        max_wait: float | None = 30.0,
        heartbeat: float | None = None,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
    ) -> MessageBatch: ...

    @overload
    async def fetch(
        self,
        *,
        max_bytes: int,
        max_wait: float | None = 30.0,
        heartbeat: float | None = None,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
    ) -> MessageBatch: ...

    async def fetch(
        self,
        *,
        max_messages: int | None = None,
        max_bytes: int | None = None,
        max_wait: float | None = 30.0,
        heartbeat: float | None = None,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer.

        Args:
            max_messages: Maximum number of messages to fetch (mutually exclusive with max_bytes)
            max_bytes: Maximum bytes to fetch (mutually exclusive with max_messages)
            max_wait: Maximum time to wait for messages in seconds
            heartbeat: Heartbeat interval in seconds
            min_ack_pending: Minimum number of unacknowledged messages that can be pending
            min_pending: Minimum number of messages that should be pending in the stream
            priority_group: Priority group for message fetching

        Returns:
            MessageBatch containing the fetched messages

        Raises:
            ValueError: If both max_messages and max_bytes are specified, or neither is specified
        """
        if (max_messages is None) == (max_bytes is None):
            raise ValueError("Must specify exactly one of max_messages or max_bytes")

        # Use max_messages as batch size, or 1 if only max_bytes is specified
        batch_size = max_messages if max_messages is not None else 1

        return await self._fetch(
            batch=batch_size,
            max_wait=max_wait,
            heartbeat=heartbeat,
            max_bytes=max_bytes,
            min_ack_pending=min_ack_pending,
            min_pending=min_pending,
            priority_group=priority_group,
            no_wait=False,
        )

    @overload
    async def fetch_nowait(
        self,
        *,
        max_messages: int,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
    ) -> MessageBatch: ...

    @overload
    async def fetch_nowait(
        self,
        *,
        max_bytes: int,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
    ) -> MessageBatch: ...

    async def fetch_nowait(
        self,
        *,
        max_messages: int | None = None,
        max_bytes: int | None = None,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
    ) -> MessageBatch:
        """Fetch messages without waiting (returns immediately).

        Args:
            max_messages: Maximum number of messages to fetch (mutually exclusive with max_bytes)
            max_bytes: Maximum bytes to fetch (mutually exclusive with max_messages)
            min_ack_pending: Minimum number of unacknowledged messages that can be pending
            min_pending: Minimum number of messages that should be pending in the stream
            priority_group: Priority group for message fetching

        Returns:
            MessageBatch containing the available messages

        Raises:
            ValueError: If both max_messages and max_bytes are specified, or neither is specified
        """
        if (max_messages is None) == (max_bytes is None):
            raise ValueError("Must specify exactly one of max_messages or max_bytes")

        # Use max_messages as batch size, or 1 if only max_bytes is specified
        batch_size = max_messages if max_messages is not None else 1

        return await self._fetch(
            batch=batch_size,
            max_wait=None,
            heartbeat=None,
            max_bytes=max_bytes,
            min_ack_pending=min_ack_pending,
            min_pending=min_pending,
            priority_group=priority_group,
            no_wait=True,
        )

    async def _fetch(
        self,
        *,
        batch: int,
        max_wait: float | None,
        heartbeat: float | None,
        max_bytes: int | None,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
        no_wait: bool,
    ) -> MessageBatch:
        jetstream = self._stream._jetstream
        inbox = new_inbox()
        subscription = await jetstream.client.subscribe(inbox)

        request: dict[str, Any] = {"batch": batch, "no_wait": no_wait}

        if max_wait is not None:
            request["expires"] = int(max_wait * 1_000_000_000)

        if heartbeat is not None:
            request["idle_heartbeat"] = int(heartbeat * 1_000_000_000)

        if max_bytes is not None:
            request["max_bytes"] = max_bytes

        if min_ack_pending is not None:
            request["min_ack_pending"] = min_ack_pending

        if min_pending is not None:
            request["min_pending"] = min_pending

        if priority_group is not None:
            request["priority_group"] = priority_group

        api_prefix = jetstream.api_prefix
        subject = f"{api_prefix}.CONSUMER.MSG.NEXT.{self.stream_name}.{self.name}"
        payload = json.dumps(request).encode()

        await jetstream.client.publish(subject, payload=payload, reply_to=inbox)

        return PullMessageBatch(subscription=subscription, batch_size=batch, jetstream=jetstream, max_wait=max_wait)
