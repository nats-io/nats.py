from __future__ import annotations

import asyncio
import json
import logging
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


logger = logging.getLogger(__name__)


class PullMessageBatch(MessageBatch):
    _subscription: Subscription
    _pending_messages: int
    _jetstream: Any
    _terminated: bool
    _error: Exception | None
    _deadline: float | None
    _heartbeat: float | None
    _heartbeat_deadline: float | None
    _heartbeat_paused: bool
    _heartbeat_remaining: float | None

    def __init__(
        self,
        subscription: Subscription,
        batch_size: int,
        jetstream,
        max_wait: float | None,
        heartbeat: float | None = None,
    ):
        self._subscription = subscription
        self._pending_messages = batch_size
        self._jetstream = jetstream
        self._terminated = False
        self._error = None
        self._deadline = time.time() + max_wait if max_wait is not None else None
        self._heartbeat = heartbeat
        self._heartbeat_deadline = time.time() + (heartbeat * 2) if heartbeat is not None else None
        self._heartbeat_paused = False
        self._heartbeat_remaining = None

        # Register disconnect/reconnect callbacks for heartbeat timer (ADR-37)
        if heartbeat is not None:
            client = jetstream._client
            client.add_disconnected_callback(self._pause_heartbeat_timer)
            client.add_reconnected_callback(self._resume_heartbeat_timer)

    def _pause_heartbeat_timer(self) -> None:
        """Pause the heartbeat timer on disconnect (ADR-37)."""
        if self._heartbeat_deadline is not None and not self._heartbeat_paused:
            self._heartbeat_remaining = self._heartbeat_deadline - time.time()
            self._heartbeat_paused = True

    def _resume_heartbeat_timer(self) -> None:
        """Resume the heartbeat timer on reconnect (ADR-37)."""
        if self._heartbeat_paused and self._heartbeat_remaining is not None:
            self._heartbeat_deadline = time.time() + self._heartbeat_remaining
            self._heartbeat_paused = False
            self._heartbeat_remaining = None

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
                # Check heartbeat timeout (ADR-37: warn at 2x idle_heartbeat)
                if self._heartbeat_deadline is not None and time.time() > self._heartbeat_deadline:
                    logger.warning(
                        "Heartbeat timeout: no message received within %.2fs (2x idle_heartbeat of %.2fs)",
                        self._heartbeat * 2,
                        self._heartbeat,
                    )
                    # Reset deadline to avoid repeated warnings
                    self._heartbeat_deadline = None

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

                # Reset heartbeat timer on any message (ADR-37)
                if self._heartbeat is not None:
                    self._heartbeat_deadline = time.time() + (self._heartbeat * 2)

                # Check for status messages
                if raw_msg.status is not None:
                    match raw_msg.status.code:
                        case "100":  # Heartbeat
                            continue
                        case "404":  # No messages
                            raise StopAsyncIteration
                        case "408":  # Timeout
                            raise StopAsyncIteration
                        case "400":  # Bad Request - terminal error
                            description = raw_msg.status.description or "Bad Request"
                            self._error = ValueError(f"Bad Request: {description}")
                            raise StopAsyncIteration
                        case "409":  # Multiple meanings - need to parse description
                            description = raw_msg.status.description or ""
                            description_lower = description.lower()

                            # Terminal 409 errors
                            if "consumer deleted" in description_lower:
                                from nats.jetstream.errors import ConsumerDeletedError

                                self._error = ConsumerDeletedError(description)
                                raise StopAsyncIteration
                            elif "consumer is push based" in description_lower:
                                self._error = ValueError(f"Consumer type mismatch: {description}")
                                raise StopAsyncIteration

                            # Non-terminal 409 errors (silently handled)
                            elif "message size exceeds maxbytes" in description_lower:
                                # Single message too large - skip and continue
                                raise StopAsyncIteration
                            elif "exceeded maxrequestbatch" in description_lower:
                                # Batch size too large - batch complete, no error
                                raise StopAsyncIteration
                            elif "exceeded maxrequestexpires" in description_lower:
                                # Expiration too long - batch complete, no error
                                raise StopAsyncIteration
                            elif "exceeded maxrequestmaxbytes" in description_lower:
                                # Byte limit too large - batch complete, no error
                                raise StopAsyncIteration
                            elif "exceeded maxwaiting" in description_lower:
                                # Too many pending requests - batch complete, no error
                                raise StopAsyncIteration
                            else:
                                # Unknown 409 error - treat as terminal
                                self._error = Exception(f"Status 409: {description}")
                                raise StopAsyncIteration
                        case _:
                            # Unknown status code - treat as terminal
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
    _min_ack_pending: int | None
    _min_pending: int | None
    _priority_group: str | None
    _priority: int | None
    _terminated: bool
    _pending_messages: int
    _pending_bytes: int
    _request_task: asyncio.Task[None] | None
    _heartbeat_task: asyncio.Task[None] | None
    _started: bool
    _heartbeat_deadline: float | None
    _heartbeat_paused: bool
    _heartbeat_remaining: float | None

    def __init__(
        self,
        consumer: PullConsumer,
        subscription: Subscription,
        batch: int = 100,
        max_bytes: int | None = None,
        heartbeat: float | None = None,
        expires: float = 30.0,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
        priority: int | None = None,
        threshold_messages: int | None = None,
        threshold_bytes: int | None = None,
    ):
        self._consumer = consumer
        self._subscription = subscription
        self._batch = batch
        self._max_bytes = max_bytes
        self._heartbeat = heartbeat
        self._expires = expires
        self._min_ack_pending = min_ack_pending
        self._min_pending = min_pending
        self._priority_group = priority_group
        self._priority = priority
        self._heartbeat_paused = False
        self._heartbeat_remaining = None
        # Set thresholds with defaults to 50% if not specified
        self._threshold_messages = threshold_messages if threshold_messages is not None else batch // 2
        self._threshold_bytes = (
            threshold_bytes
            if threshold_bytes is not None and max_bytes is not None
            else (max_bytes // 2 if max_bytes is not None else None)
        )
        self._terminated = False
        self._pending_messages = 0
        self._pending_bytes = 0
        self._request_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._started = False
        self._heartbeat_deadline = time.time() + (heartbeat * 2) if heartbeat is not None else None

        # Register disconnect/reconnect callbacks for heartbeat timer (ADR-37)
        if heartbeat is not None:
            client = consumer._stream._jetstream._client
            client.add_disconnected_callback(self._pause_heartbeat_timer)
            client.add_reconnected_callback(self._resume_heartbeat_timer)

    def _pause_heartbeat_timer(self) -> None:
        """Pause the heartbeat timer on disconnect (ADR-37)."""
        if self._heartbeat_deadline is not None and not self._heartbeat_paused:
            self._heartbeat_remaining = self._heartbeat_deadline - time.time()
            self._heartbeat_paused = True

    def _resume_heartbeat_timer(self) -> None:
        """Resume the heartbeat timer on reconnect (ADR-37)."""
        if self._heartbeat_paused and self._heartbeat_remaining is not None:
            self._heartbeat_deadline = time.time() + self._heartbeat_remaining
            self._heartbeat_paused = False
            self._heartbeat_remaining = None

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
            # Start heartbeat monitoring if configured
            if self._heartbeat is not None:
                self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

        # Get next message from subscription
        while True:
            try:
                raw_msg: ClientMessage = await self._subscription.next()
            except Exception:
                await self._cleanup()
                raise StopAsyncIteration

            # Reset heartbeat timer on any message (ADR-37)
            if self._heartbeat is not None:
                self._heartbeat_deadline = time.time() + (self._heartbeat * 2)

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
                    case "408" | "409":  # Timeout or request terminated
                        # Read pending headers (ADR-37) - server tells us how many messages/bytes
                        # from our request were NOT delivered
                        if raw_msg.headers:
                            if pending := raw_msg.headers.get("Nats-Pending-Messages"):
                                self._pending_messages = max(0, self._pending_messages - int(pending))
                            if pending := raw_msg.headers.get("Nats-Pending-Bytes"):
                                self._pending_bytes = max(0, self._pending_bytes - int(pending))

                        # For 409, check if this is a terminal error
                        if raw_msg.status.code == "409":
                            description = raw_msg.status.description or ""
                            description_lower = description.lower()

                            # Terminal 409 errors
                            if "consumer deleted" in description_lower or "consumer is push based" in description_lower:
                                await self._cleanup()
                                raise StopAsyncIteration

                        # Non-terminal - request more messages
                        await self._send_request()
                        continue
                    case "400":  # Bad Request - terminal error
                        await self._cleanup()
                        raise StopAsyncIteration
                    case _:  # Unknown status - terminal
                        await self._cleanup()
                        raise StopAsyncIteration

            # Track that we consumed a message
            self._pending_messages = max(0, self._pending_messages - 1)
            # Calculate full message size: subject + reply + headers + payload (ADR-37)
            self._pending_bytes = max(0, self._pending_bytes - len(raw_msg))

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

        if self._min_ack_pending is not None:
            request["min_ack_pending"] = self._min_ack_pending

        if self._min_pending is not None:
            request["min_pending"] = self._min_pending

        if self._priority_group is not None:
            request["group"] = self._priority_group

        if self._priority is not None:
            request["priority"] = self._priority

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
                should_request = self._pending_messages < self._threshold_messages

                # Also consider bytes if we have a limit and threshold
                if self._threshold_bytes is not None:
                    should_request = should_request or self._pending_bytes < self._threshold_bytes

                if should_request:
                    await self._send_request()
        except asyncio.CancelledError:
            pass
        except Exception:
            # If request loop fails, terminate the stream
            self._terminated = True

    async def _heartbeat_monitor(self):
        """Background task that monitors for heartbeat timeouts (ADR-37)."""
        try:
            while not self._terminated:
                await asyncio.sleep(0.1)  # Check every 100ms

                # Check if heartbeat timeout has been reached (2x idle_heartbeat)
                if self._heartbeat_deadline is not None and time.time() > self._heartbeat_deadline:
                    logger.warning(
                        "Heartbeat timeout: no message received within %.2fs (2x idle_heartbeat of %.2fs)",
                        self._heartbeat * 2,
                        self._heartbeat,
                    )
                    # Reset pending counts and request more messages (non-terminal)
                    self._pending_messages = 0
                    self._pending_bytes = 0
                    await self._send_request()
                    # Reset deadline after warning to avoid repeated warnings
                    self._heartbeat_deadline = time.time() + (self._heartbeat * 2)
        except asyncio.CancelledError:
            pass
        except Exception:
            # If heartbeat monitor fails, don't terminate the stream
            pass

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

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

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
        priority: int | None = None,
    ) -> Message:
        """Fetch the next single message from the consumer.

        Args:
            max_wait: Maximum time to wait for a message in seconds
            heartbeat: Heartbeat interval in seconds
            min_ack_pending: Minimum number of unacknowledged messages that can be pending
            min_pending: Minimum number of messages that should be pending in the stream
            priority_group: Priority group for message fetching
            priority: Priority level (0-9, lower is higher priority). Requires priority_group
                     and consumer with PriorityPolicyPrioritized.

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
            priority=priority,
        )
        async for msg in batch:
            return msg

        # Check if there was an error during fetch
        if batch.error is not None:
            raise batch.error

        raise asyncio.TimeoutError("No message received within timeout")

    async def messages(
        self,
        *,
        max_messages: int | None = None,
        max_bytes: int | None = None,
        heartbeat: float | None = None,
        max_wait: float = 30.0,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
        priority: int | None = None,
        threshold_messages: int | None = None,
        threshold_bytes: int | None = None,
    ) -> MessageStream:
        """Create a continuous message stream for manual iteration.

        Args:
            max_messages: Maximum number of messages to request per batch
            max_bytes: Maximum bytes per batch (can be used together with max_messages)
            heartbeat: Heartbeat interval in seconds
            max_wait: Request expiration time in seconds
            min_ack_pending: Minimum number of unacknowledged messages that can be pending
            min_pending: Minimum number of messages that should be pending in the stream
            priority_group: Priority group for message fetching
            priority: Priority level (0-9, lower is higher priority). Requires priority_group
                     and consumer with PriorityPolicyPrioritized.
            threshold_messages: Refill threshold for messages (default: max_messages // 2).
                              When pending messages falls below this, a new batch is requested.
            threshold_bytes: Refill threshold for bytes (default: max_bytes // 2).
                           When pending bytes falls below this, a new batch is requested.

        Returns:
            MessageStream for manual message consumption
        """
        # ADR-37: Users cannot specify both max_messages and max_bytes simultaneously
        if max_messages is not None and max_bytes is not None:
            raise ValueError("Cannot specify both max_messages and max_bytes simultaneously")

        jetstream = self._stream._jetstream
        inbox = new_inbox()
        subscription = await jetstream.client.subscribe(inbox)

        # ADR-37: When using max_bytes, set batch to large value (1,000,000) for better throughput
        batch = max_messages if max_messages is not None else (1_000_000 if max_bytes is not None else 100)

        stream = PullMessageStream(
            consumer=self,
            subscription=subscription,
            batch=batch,
            max_bytes=max_bytes,
            heartbeat=heartbeat,
            expires=max_wait,
            min_ack_pending=min_ack_pending,
            min_pending=min_pending,
            priority_group=priority_group,
            priority=priority,
            threshold_messages=threshold_messages,
            threshold_bytes=threshold_bytes,
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
        priority: int | None = None,
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
        priority: int | None = None,
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
        priority: int | None = None,
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
            priority: Priority level (0-9, lower is higher priority). Requires priority_group
                     and consumer with PriorityPolicyPrioritized.

        Returns:
            MessageBatch containing the fetched messages

        Raises:
            ValueError: If both max_messages and max_bytes are specified, or neither is specified
        """
        if (max_messages is None) == (max_bytes is None):
            raise ValueError("Must specify exactly one of max_messages or max_bytes")

        # ADR-37: When using max_bytes, set batch to large value (1,000,000) for better throughput
        batch_size = max_messages if max_messages is not None else 1_000_000

        return await self._fetch(
            batch=batch_size,
            max_wait=max_wait,
            heartbeat=heartbeat,
            max_bytes=max_bytes,
            min_ack_pending=min_ack_pending,
            min_pending=min_pending,
            priority_group=priority_group,
            priority=priority,
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
        priority: int | None = None,
    ) -> MessageBatch: ...

    @overload
    async def fetch_nowait(
        self,
        *,
        max_bytes: int,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
        priority: int | None = None,
    ) -> MessageBatch: ...

    async def fetch_nowait(
        self,
        *,
        max_messages: int | None = None,
        max_bytes: int | None = None,
        min_ack_pending: int | None = None,
        min_pending: int | None = None,
        priority_group: str | None = None,
        priority: int | None = None,
    ) -> MessageBatch:
        """Fetch messages without waiting (returns immediately).

        Args:
            max_messages: Maximum number of messages to fetch (mutually exclusive with max_bytes)
            max_bytes: Maximum bytes to fetch (mutually exclusive with max_messages)
            min_ack_pending: Minimum number of unacknowledged messages that can be pending
            min_pending: Minimum number of messages that should be pending in the stream
            priority_group: Priority group for message fetching
            priority: Priority level (0-9, lower is higher priority). Requires priority_group
                     and consumer with PriorityPolicyPrioritized.

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
            priority=priority,
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
        priority: int | None = None,
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
            request["group"] = priority_group

        if priority is not None:
            request["priority"] = priority

        api_prefix = jetstream.api_prefix
        subject = f"{api_prefix}.CONSUMER.MSG.NEXT.{self.stream_name}.{self.name}"
        payload = json.dumps(request).encode()

        await jetstream.client.publish(subject, payload=payload, reply_to=inbox)

        return PullMessageBatch(
            subscription=subscription, batch_size=batch, jetstream=jetstream, max_wait=max_wait, heartbeat=heartbeat
        )
