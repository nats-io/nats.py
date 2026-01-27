"""Message definitions for JetStream."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from nats.client.message import Headers

from .util import new_inbox

# Avoid circular imports
if TYPE_CHECKING:
    from . import JetStream


@dataclass
class SequencePair:
    """Sequence information for a message."""

    consumer: int
    stream: int


@dataclass
class Metadata:
    """Metadata for a JetStream message."""

    sequence: SequencePair
    """Sequence information for the message (consumer and stream sequences)."""

    num_delivered: int
    """Number of times this message has been delivered to the consumer."""

    num_pending: int
    """Number of messages that match the consumer's filter but haven't been delivered yet."""

    timestamp: datetime
    """Time when the message was originally stored on the stream."""

    stream: str
    """Name of the stream this message is stored on."""

    consumer: str
    """Name of the consumer this message was delivered to."""

    domain: str = ""
    """Domain this message was received on (empty string if not set)."""

    @classmethod
    def from_reply(cls, reply_subject: str) -> Metadata:
        """Parse metadata from a JetStream reply subject.

        The reply subject contains the message metadata in one of three forms:
        - V2 (12 tokens): $JS.ACK.<domain>.<account_hash>.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<timestamp>.<pending>.<random>
        - V1 (9 tokens): $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<timestamp>.<pending>
        - V1 (8 tokens): $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<timestamp>

        Args:
            reply_subject: The JetStream ACK reply subject to parse.

        Returns:
            Parsed metadata from the reply subject.

        Raises:
            ValueError: If the reply subject format is invalid or not recognized.
        """
        tokens = reply_subject.split(".")

        match tokens:
            case [
                "$JS",
                "ACK",
                domain,
                _account_hash,
                stream,
                consumer,
                delivered,
                stream_seq,
                consumer_seq,
                timestamp,
                pending,
                *_,
            ]:
                # V2 format (12+ tokens) with domain and account hash
                domain_value = "" if domain == "_" else domain
                return cls(
                    sequence=SequencePair(consumer=int(consumer_seq), stream=int(stream_seq)),
                    num_delivered=int(delivered),
                    num_pending=int(pending),
                    timestamp=datetime.fromtimestamp(int(timestamp) / 1e9, tz=timezone.utc),
                    stream=stream,
                    consumer=consumer,
                    domain=domain_value,
                )
            case ["$JS", "ACK", stream, consumer, delivered, stream_seq, consumer_seq, timestamp, pending]:
                # V1 format (9 tokens) with pending count
                return cls(
                    sequence=SequencePair(consumer=int(consumer_seq), stream=int(stream_seq)),
                    num_delivered=int(delivered),
                    num_pending=int(pending),
                    timestamp=datetime.fromtimestamp(int(timestamp) / 1e9, tz=timezone.utc),
                    stream=stream,
                    consumer=consumer,
                )
            case ["$JS", "ACK", stream, consumer, delivered, stream_seq, consumer_seq, timestamp]:
                # V1 format (8 tokens) without pending count
                return cls(
                    sequence=SequencePair(consumer=int(consumer_seq), stream=int(stream_seq)),
                    num_delivered=int(delivered),
                    num_pending=0,
                    timestamp=datetime.fromtimestamp(int(timestamp) / 1e9, tz=timezone.utc),
                    stream=stream,
                    consumer=consumer,
                )
            case _:
                raise ValueError(f"Invalid JetStream ACK reply subject format: {reply_subject}")


class Message:
    """A message received from a JetStream consumer."""

    _data: bytes
    _subject: str
    _reply_to: str | None
    _headers: Headers | None
    _metadata: Metadata
    _jetstream: JetStream | None

    def __init__(
        self,
        data: bytes,
        subject: str,
        reply_to: str | None,
        headers: Headers | None,
        metadata: Metadata | None = None,
        jetstream: JetStream | None = None,
    ) -> None:
        """Initialize a JetStream message.

        Args:
            data: The message payload.
            subject: The subject the message was published on.
            reply_to: The reply subject for acknowledgments.
            headers: Optional message headers.
            metadata: Optional explicit metadata (will be parsed from reply_to if not provided).
            jetstream: Reference to the JetStream client for acknowledgments.
        """
        self._data = data
        self._subject = subject
        self._reply_to = reply_to
        self._headers = headers
        self._jetstream = jetstream

        # Initialize with default metadata
        self._metadata = Metadata(
            sequence=SequencePair(consumer=0, stream=0),
            num_delivered=0,
            num_pending=0,
            timestamp=datetime.now(timezone.utc),
            stream="",
            consumer="",
        )

        # Override with parsed metadata from reply if available
        if reply_to:
            try:
                self._metadata = Metadata.from_reply(reply_to)
            except ValueError:
                # Invalid reply subject format, keep default metadata
                pass

        # Override with explicitly provided metadata if available
        if metadata is not None:
            self._metadata = metadata

    @property
    def metadata(self) -> Metadata:
        """Return metadata for the JetStream message."""
        return self._metadata

    @property
    def data(self) -> bytes:
        """Return the message body."""
        return self._data

    @property
    def headers(self) -> Headers | None:
        """Return the message headers, or None if no headers exist."""
        return self._headers

    @property
    def subject(self) -> str:
        """Return the subject on which the message was published/received."""
        return self._subject

    @property
    def reply_to(self) -> str:
        """Return the reply subject for the message, or empty string if not set."""
        return self._reply_to or ""

    async def ack(self) -> None:
        """Acknowledge a message.

        This tells the server that the message was successfully processed
        and it can move on to the next message.

        Raises:
            ValueError: If reply_to or jetstream reference is missing.
        """
        if not self._reply_to:
            raise ValueError("Cannot acknowledge message without reply_to")
        if not self._jetstream:
            raise ValueError("Cannot acknowledge message without JetStream reference")

        await self._jetstream.client.publish(self._reply_to, b"")

    async def double_ack(self, *, timeout: float = 1.0) -> None:
        """Acknowledge a message and wait for server confirmation.

        While it impacts performance, it is useful for scenarios where
        message loss is not acceptable.

        Args:
            timeout: Maximum time to wait for acknowledgment confirmation (in seconds).

        Raises:
            TimeoutError: If the server doesn't respond within the timeout period.
            ValueError: If reply_to or jetstream reference is missing.
        """
        if not self._reply_to:
            raise ValueError("Cannot acknowledge message without reply_to")
        if not self._jetstream:
            raise ValueError("Cannot acknowledge message without JetStream reference")

        client = self._jetstream.client
        inbox = new_inbox()
        subscription = await client.subscribe(inbox)

        try:
            await client.publish(self._reply_to, b"", reply_to=inbox)
            await subscription.next(timeout=timeout)
        finally:
            await subscription.unsubscribe()

    async def nak(self) -> None:
        """Negatively acknowledge a message.

        This tells the server to redeliver the message immediately.

        Note: NAK triggers instant redelivery and does not respect the AckWait
        or Backoff settings configured on the consumer. For delayed redelivery,
        use nak_with_delay().

        Raises:
            ValueError: If reply_to or jetstream reference is missing.
        """
        if not self._reply_to:
            raise ValueError("Cannot NAK message without reply_to")
        if not self._jetstream:
            raise ValueError("Cannot NAK message without JetStream reference")

        await self._jetstream.client.publish(self._reply_to, b"-NAK")

    async def nak_with_delay(self, delay: float) -> None:
        """Negatively acknowledge a message with a redelivery delay.

        Args:
            delay: The delay in seconds before the message should be redelivered.

        Raises:
            ValueError: If reply_to or jetstream reference is missing.
        """
        if not self._reply_to:
            raise ValueError("Cannot NAK message without reply_to")
        if not self._jetstream:
            raise ValueError("Cannot NAK message without JetStream reference")

        # Convert delay to nanoseconds for NATS
        delay_ns = int(delay * 1_000_000_000)
        payload = f'-NAK {{"delay": {delay_ns}}}'.encode()
        await self._jetstream.client.publish(self._reply_to, payload)

    async def in_progress(self) -> None:
        """Signal that this message is still being processed.

        This resets the redelivery timer on the server, preventing the message
        from being redelivered while work is in progress.

        Raises:
            ValueError: If reply_to or jetstream reference is missing.
        """
        if not self._reply_to:
            raise ValueError("Cannot send in-progress for message without reply_to")
        if not self._jetstream:
            raise ValueError("Cannot send in-progress for message without JetStream reference")

        await self._jetstream.client.publish(self._reply_to, b"+WPI")

    async def term(self) -> None:
        """Terminate this message, preventing any further redelivery.

        The server will not redeliver this message regardless of the
        MaxDeliver setting on the consumer.

        Raises:
            ValueError: If reply_to or jetstream reference is missing.
        """
        if not self._reply_to:
            raise ValueError("Cannot terminate message without reply_to")
        if not self._jetstream:
            raise ValueError("Cannot terminate message without JetStream reference")

        await self._jetstream.client.publish(self._reply_to, b"+TERM")

    async def term_with_reason(self, reason: str) -> None:
        """Terminate this message with an explanation.

        The server will not redeliver this message regardless of the MaxDeliver
        setting on the consumer. The provided reason will be included in the
        JetStream advisory event.

        Args:
            reason: Explanation for why the message is being terminated.

        Note: Requires JetStream server version 2.10.4 or later. On older servers,
        this method will be ignored and the message will not be terminated.

        Raises:
            ValueError: If reply_to or jetstream reference is missing.
        """
        if not self._reply_to:
            raise ValueError("Cannot terminate message without reply_to")
        if not self._jetstream:
            raise ValueError("Cannot terminate message without JetStream reference")

        payload = f"+TERM {reason}".encode()
        await self._jetstream.client.publish(self._reply_to, payload)


__all__ = [
    "Headers",
    "Message",
    "Metadata",
    "SequencePair",
]
