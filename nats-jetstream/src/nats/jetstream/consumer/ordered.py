"""Ordered consumer implementation for JetStream."""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, AsyncIterator

from nats.jetstream.consumer import (
    Consumer,
    ConsumerConfig,
    ConsumerInfo,
    MessageBatch,
    MessageStream,
    OrderedConsumerConfig,
)
from nats.jetstream.consumer.pull import PullConsumer, PullMessageStream
from nats.jetstream.errors import OrderedConsumerClosedError, OrderedConsumerResetError
from nats.jetstream.message import Message

if TYPE_CHECKING:
    from nats.jetstream import JetStream
    from nats.jetstream.stream import Stream

logger = logging.getLogger(__name__)


@dataclass
class _Cursor:
    stream_seq: int = 0
    deliver_seq: int = 0


class OrderedConsumer(Consumer):
    """An ordered consumer that guarantees in-order message delivery.

    Ordered consumers are ephemeral, client-managed pull consumers. When a
    sequence gap, heartbeat miss, or consumer deletion is detected, the library
    automatically recreates the underlying server-side consumer from the last
    successfully consumed stream sequence.
    """

    _stream: Stream
    _cfg: OrderedConsumerConfig
    _current_consumer: PullConsumer | None
    _cursor: _Cursor
    _name_prefix: str
    _serial: int
    _closed: bool
    _consumer_type: str | None  # None, "consume", or "fetch"
    _last_batch: _OrderedMessageBatch | None
    _delete_tasks: set[asyncio.Task[None]]

    def __init__(
        self,
        stream: Stream,
        cfg: OrderedConsumerConfig,
    ) -> None:
        self._stream = stream
        self._cfg = cfg
        self._current_consumer = None
        self._cursor = _Cursor()
        self._name_prefix = cfg.name_prefix or uuid.uuid4().hex
        self._serial = 0
        self._closed = False
        self._consumer_type = None
        self._last_batch = None
        self._delete_tasks = set()

    @classmethod
    async def create(cls, stream: Stream, cfg: OrderedConsumerConfig) -> OrderedConsumer:
        """Create an ordered consumer with its initial server-side consumer."""
        oc = cls(stream, cfg)

        # Build initial consumer config and create the server-side consumer
        consumer_config = oc._get_consumer_config()
        consumer = await stream.create_or_update_consumer(consumer_config)
        oc._current_consumer = consumer  # type: ignore[assignment]

        return oc

    @property
    def name(self) -> str:
        if self._current_consumer is None:
            return f"{self._name_prefix}_0"
        return self._current_consumer.name

    @property
    def stream_name(self) -> str:
        return self._stream.name

    @property
    def info(self) -> ConsumerInfo:
        if self._current_consumer is None:
            raise OrderedConsumerClosedError("Ordered consumer has no active consumer")
        return self._current_consumer.info

    async def get_info(self) -> ConsumerInfo:
        if self._current_consumer is None:
            raise OrderedConsumerClosedError("Ordered consumer has no active consumer")
        return await self._current_consumer.get_info()

    async def _prepare_fetch(self) -> None:
        """Validate state and reset consumer if needed before a fetch operation."""
        if self._closed:
            raise OrderedConsumerClosedError("Ordered consumer is closed")
        if self._consumer_type == "consume":
            raise RuntimeError("Cannot use fetch on an ordered consumer already used with messages()")

        self._consumer_type = "fetch"

        # Update cursor from previous fetch batch, then reset consumer.
        # On the very first fetch, skip reset and use the initially-created consumer.
        if self._last_batch is not None:
            last_sseq = self._last_batch.last_stream_seq
            if last_sseq > 0:
                self._cursor.stream_seq = last_sseq
            await self._reset()

    async def fetch(
        self,
        *,
        max_messages: int | None = None,
        max_bytes: int | None = None,
        max_wait: float | None = 30.0,
        heartbeat: float | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages.

        Note: Each fetch call resets the ordered consumer, which is less
        efficient than using messages() for continuous consumption.
        """
        await self._prepare_fetch()

        batch = await self._current_consumer.fetch(
            max_messages=max_messages,
            max_bytes=max_bytes,
            max_wait=max_wait,
            heartbeat=heartbeat,
        )

        ordered_batch = _OrderedMessageBatch(self, batch)
        self._last_batch = ordered_batch
        return ordered_batch

    async def fetch_nowait(
        self,
        *,
        max_messages: int | None = None,
        max_bytes: int | None = None,
    ) -> MessageBatch:
        """Fetch available messages without waiting.

        Note: Each fetch call resets the ordered consumer, which is less
        efficient than using messages() for continuous consumption.
        """
        await self._prepare_fetch()

        batch = await self._current_consumer.fetch_nowait(
            max_messages=max_messages,
            max_bytes=max_bytes,
        )

        ordered_batch = _OrderedMessageBatch(self, batch)
        self._last_batch = ordered_batch
        return ordered_batch

    async def next(self, max_wait: float = 5.0) -> Message:
        """Fetch the next single message.

        Note: Each call resets the ordered consumer. Use messages() for
        efficient continuous consumption.
        """
        batch = await self.fetch(max_messages=1, max_wait=max_wait)
        async for msg in batch:
            return msg

        if batch.error is not None:
            raise batch.error

        raise asyncio.TimeoutError("No message received within timeout")

    async def messages(
        self,
        *,
        heartbeat: float | None = None,
        max_wait: float | None = None,
        max_messages: int | None = None,
        max_bytes: int | None = None,
    ) -> AsyncIterator[Message]:
        """Get an async iterator for continuous ordered message consumption.

        This is the most efficient way to consume messages from an ordered
        consumer. On sequence gaps or errors, the consumer is automatically
        recreated and consumption resumes from the correct position.
        """
        if self._closed:
            raise OrderedConsumerClosedError("Ordered consumer is closed")
        if self._consumer_type == "fetch":
            raise RuntimeError("Cannot use messages on an ordered consumer already used with fetch()")

        self._consumer_type = "consume"

        return _OrderedMessageStream(
            consumer=self,
            heartbeat=heartbeat,
            max_wait=max_wait,
            max_messages=max_messages,
            max_bytes=max_bytes,
        )

    def _get_consumer_config(self) -> ConsumerConfig:
        """Build the server-side ConsumerConfig for the current serial."""
        self._serial += 1
        name = f"{self._name_prefix}_{self._serial}"

        # Determine start sequence
        if self._cursor.stream_seq == 0:
            # No messages consumed yet - use user's deliver policy
            opt_start_seq = self._cfg.opt_start_seq
            deliver_policy = self._cfg.deliver_policy
            opt_start_time = self._cfg.opt_start_time

            # For certain policies, opt_start_seq should not be set
            if deliver_policy in ("last_per_subject", "last", "new", "all"):
                opt_start_seq = None
            elif deliver_policy == "by_start_time":
                opt_start_seq = None
            # For by_start_sequence, keep the user's opt_start_seq
        else:
            # Resume from next sequence after last consumed
            deliver_policy = "by_start_sequence"
            opt_start_seq = self._cursor.stream_seq + 1
            opt_start_time = None

        inactive_threshold = self._cfg.inactive_threshold or timedelta(minutes=5)

        filter_subject = None
        filter_subjects = None
        if self._cfg.filter_subjects:
            if len(self._cfg.filter_subjects) == 1:
                filter_subject = self._cfg.filter_subjects[0]
            else:
                filter_subjects = self._cfg.filter_subjects

        # For last_per_subject with no filter, use ">" to match all
        if deliver_policy == "last_per_subject" and not self._cfg.filter_subjects:
            filter_subjects = [">"]

        return ConsumerConfig(
            name=name,
            deliver_policy=deliver_policy,
            opt_start_seq=opt_start_seq,
            opt_start_time=opt_start_time,
            ack_policy="none",
            inactive_threshold=inactive_threshold,
            num_replicas=1,
            headers_only=self._cfg.headers_only or None,
            mem_storage=True,
            metadata=self._cfg.metadata,
            replay_policy=self._cfg.replay_policy,
            filter_subject=filter_subject,
            filter_subjects=filter_subjects,
        )

    async def _reset(self) -> None:
        """Reset the ordered consumer by deleting old and creating new."""
        if self._closed:
            raise OrderedConsumerClosedError("Ordered consumer is closed")

        # Delete old consumer (fire-and-forget)
        if self._current_consumer is not None:
            old_name = self._current_consumer.name
            task = asyncio.create_task(self._delete_consumer(old_name))
            self._delete_tasks.add(task)
            task.add_done_callback(self._delete_tasks.discard)

        # Reset delivery sequence tracking
        self._cursor.deliver_seq = 0

        # Build new config
        config = self._get_consumer_config()

        # Create new consumer with exponential backoff retry
        interval = 1.0
        max_interval = 10.0
        factor = 2.0
        attempts = 0

        while True:
            if self._closed:
                raise OrderedConsumerClosedError("Ordered consumer is closed")

            try:
                consumer = await self._stream.create_or_update_consumer(config)
                self._current_consumer = consumer  # type: ignore[assignment]
                return
            except Exception as e:
                attempts += 1
                max_attempts = self._cfg.max_reset_attempts
                if max_attempts > 0 and attempts >= max_attempts:
                    raise OrderedConsumerResetError(
                        f"Failed to reset ordered consumer after {attempts} attempts: {e}"
                    ) from e

                logger.warning(
                    "Ordered consumer reset attempt %d failed: %s. Retrying in %.1fs",
                    attempts,
                    e,
                    interval,
                )

                await asyncio.sleep(interval)
                interval = min(interval * factor, max_interval)

    async def _delete_consumer(self, name: str) -> None:
        """Delete a server-side consumer, ignoring errors."""
        try:
            await self._stream.delete_consumer(name)
        except Exception:
            pass


class _OrderedMessageBatch(MessageBatch):
    """Wrapper around a PullMessageBatch that tracks stream sequence for ordered consumers."""

    def __init__(self, consumer: OrderedConsumer, inner: MessageBatch) -> None:
        self._consumer = consumer
        self._inner = inner
        self.last_stream_seq: int = 0

    @property
    def error(self) -> Exception | None:
        return self._inner.error

    def __aiter__(self) -> AsyncIterator[Message]:
        return self

    async def __anext__(self) -> Message:
        msg = await self._inner.__anext__()
        # Track the last consumed stream sequence
        self.last_stream_seq = msg.metadata.sequence.stream
        return msg


class _OrderedMessageStream(MessageStream):
    """Ordered message stream that validates sequence and auto-resets on gaps."""

    def __init__(
        self,
        consumer: OrderedConsumer,
        heartbeat: float | None = None,
        max_wait: float | None = None,
        max_messages: int | None = None,
        max_bytes: int | None = None,
    ) -> None:
        self._consumer = consumer
        self._heartbeat = heartbeat
        self._max_wait = max_wait
        self._max_messages = max_messages
        self._max_bytes = max_bytes
        self._inner: PullMessageStream | None = None
        self._closed = False

    async def _create_inner_stream(self) -> PullMessageStream:
        """Create a new inner PullMessageStream from the current consumer."""
        stream = await self._consumer._current_consumer.messages(
            heartbeat=self._heartbeat,
            max_wait=self._max_wait if self._max_wait is not None else 30.0,
            max_messages=self._max_messages,
            max_bytes=self._max_bytes,
        )
        return stream  # type: ignore[return-value]

    def __aiter__(self) -> AsyncIterator[Message]:
        return self

    async def __anext__(self) -> Message:
        if self._closed:
            raise StopAsyncIteration

        # Create inner stream on first access
        if self._inner is None:
            self._inner = await self._create_inner_stream()

        while True:
            try:
                msg = await self._inner.__anext__()
            except StopAsyncIteration:
                # Inner stream ended - this could be heartbeat miss or other error
                # Try to reset and recreate
                if self._closed or self._consumer._closed:
                    raise StopAsyncIteration

                try:
                    await self._consumer._reset()
                    self._inner = await self._create_inner_stream()
                    continue
                except (OrderedConsumerClosedError, OrderedConsumerResetError):
                    self._closed = True
                    raise StopAsyncIteration

            # Validate sequence - consumer_seq must be consecutive
            dseq = msg.metadata.sequence.consumer
            expected = self._consumer._cursor.deliver_seq + 1

            if dseq != expected:
                logger.debug(
                    "Ordered consumer sequence mismatch: expected %d, got %d. Resetting.",
                    expected,
                    dseq,
                )
                # Stop old inner stream
                if self._inner is not None:
                    await self._inner.stop()

                try:
                    await self._consumer._reset()
                    self._inner = await self._create_inner_stream()
                    continue
                except (OrderedConsumerClosedError, OrderedConsumerResetError):
                    self._closed = True
                    raise StopAsyncIteration

            # Update cursor
            self._consumer._cursor.deliver_seq = dseq
            self._consumer._cursor.stream_seq = msg.metadata.sequence.stream
            return msg

    async def stop(self) -> None:
        """Stop the ordered message stream."""
        self._closed = True
        if self._inner is not None:
            await self._inner.stop()
