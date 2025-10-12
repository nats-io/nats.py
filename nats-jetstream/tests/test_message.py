"""Tests for the nats.jetstream.message module."""

import asyncio
from datetime import datetime, timezone

import pytest
from nats.client import Client
from nats.jetstream import JetStream
from nats.jetstream.message import Headers, Message, Metadata, SequencePair


def test_metadata_from_reply_parses_valid_subject():
    """Test parsing valid metadata from reply subject."""
    reply = "$JS.ACK.test-stream.test-consumer.1.100.50.1234567890.10"
    metadata = Metadata.from_reply(reply)

    assert metadata is not None
    assert metadata.stream == "test-stream"
    assert metadata.consumer == "test-consumer"
    assert metadata.num_delivered == 1
    assert metadata.sequence.stream == 100
    assert metadata.sequence.consumer == 50
    assert metadata.timestamp == datetime.fromtimestamp(1234567890 / 1e9, tz=timezone.utc)
    assert metadata.num_pending == 10


def test_metadata_from_reply_handles_missing_pending_count():
    """Test parsing metadata without pending count."""
    reply = "$JS.ACK.stream.consumer.2.200.100.9876543210"
    metadata = Metadata.from_reply(reply)

    assert metadata is not None
    assert metadata.stream == "stream"
    assert metadata.consumer == "consumer"
    assert metadata.num_delivered == 2
    assert metadata.sequence.stream == 200
    assert metadata.sequence.consumer == 100
    assert metadata.num_pending == 0


def test_metadata_from_reply_parses_v2_format_with_domain():
    """Test parsing V2 format with domain and account hash."""
    reply = "$JS.ACK.hub.ACCOUNT123.test-stream.test-consumer.1.100.50.1234567890.10.random-token"
    metadata = Metadata.from_reply(reply)

    assert metadata is not None
    assert metadata.stream == "test-stream"
    assert metadata.consumer == "test-consumer"
    assert metadata.num_delivered == 1
    assert metadata.sequence.stream == 100
    assert metadata.sequence.consumer == 50
    assert metadata.timestamp == datetime.fromtimestamp(1234567890 / 1e9, tz=timezone.utc)
    assert metadata.num_pending == 10
    assert metadata.domain == "hub"


def test_metadata_from_reply_handles_v2_format_with_underscore_domain():
    """Test parsing V2 format with underscore domain (should convert to empty string)."""
    reply = "$JS.ACK._.ACCOUNT123.stream.consumer.2.200.100.9876543210.5.random"
    metadata = Metadata.from_reply(reply)

    assert metadata is not None
    assert metadata.stream == "stream"
    assert metadata.consumer == "consumer"
    assert metadata.num_delivered == 2
    assert metadata.sequence.stream == 200
    assert metadata.sequence.consumer == 100
    assert metadata.num_pending == 5
    assert metadata.domain == ""


def test_metadata_from_reply_handles_v2_format_with_extra_tokens():
    """Test parsing V2 format with more than 12 tokens."""
    reply = "$JS.ACK.domain.ACCT.stream.consumer.1.50.25.1111111111.3.token1.token2.token3"
    metadata = Metadata.from_reply(reply)

    assert metadata is not None
    assert metadata.stream == "stream"
    assert metadata.consumer == "consumer"
    assert metadata.domain == "domain"


def test_metadata_from_reply_raises_error_for_invalid_subjects():
    """Test parsing invalid reply subjects raises ValueError."""
    invalid_replies = [
        "",
        "invalid",
        "$JS.NACK",
        "$JS.ACK",
        "$JS.ACK.stream",
        "$JS.ACK.stream.consumer",
        "$JS.ACK.stream.consumer.1",
    ]

    for reply in invalid_replies:
        with pytest.raises(ValueError, match="Invalid JetStream ACK reply subject format"):
            Metadata.from_reply(reply)


def test_message_creation_with_basic_data():
    """Test creating a message with basic data."""
    msg = Message(
        data=b"test data",
        subject="test.subject",
        reply_to="test.reply",
        headers=Headers({"X-Test": ["value"]}),
    )

    assert msg.data == b"test data"
    assert msg.subject == "test.subject"
    assert msg.reply_to == "test.reply"
    assert msg.headers == Headers({"X-Test": ["value"]})


def test_message_with_explicit_metadata():
    """Test creating a message with explicit metadata."""
    metadata = Metadata(
        sequence=SequencePair(consumer=10, stream=20),
        num_delivered=3,
        num_pending=5,
        timestamp=datetime.now(timezone.utc),
        stream="my-stream",
        consumer="my-consumer",
    )

    msg = Message(
        data=b"test",
        subject="test",
        reply_to=None,
        headers=None,
        metadata=metadata,
    )

    msg_metadata = msg.metadata
    assert msg_metadata.sequence.consumer == 10
    assert msg_metadata.sequence.stream == 20
    assert msg_metadata.num_delivered == 3
    assert msg_metadata.num_pending == 5
    assert msg_metadata.stream == "my-stream"
    assert msg_metadata.consumer == "my-consumer"


def test_message_metadata_parsed_from_reply_subject():
    """Test that metadata is parsed from reply subject."""
    reply = "$JS.ACK.stream.consumer.1.100.50.1234567890.10"
    msg = Message(
        data=b"test",
        subject="test",
        reply_to=reply,
        headers=None,
    )

    metadata = msg.metadata
    assert metadata.stream == "stream"
    assert metadata.consumer == "consumer"
    assert metadata.sequence.stream == 100
    assert metadata.sequence.consumer == 50


def test_message_headers_returns_none_when_not_set():
    """Test message with no headers returns None."""
    msg = Message(
        data=b"test",
        subject="test",
        reply_to=None,
        headers=None,
    )

    assert msg.headers is None


def test_message_reply_to_returns_empty_string_when_not_set():
    """Test that empty reply_to returns empty string."""
    msg = Message(
        data=b"test",
        subject="test",
        reply_to=None,
        headers=None,
    )

    assert msg.reply_to == ""


@pytest.mark.asyncio
async def test_message_ack_acknowledges_successfully(jetstream: JetStream):
    """Test message acknowledgment."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test-ack-stream",
        subjects=["test.ack"],
    )

    # Create a consumer
    consumer = await stream.create_consumer(name="test-ack-consumer", ack_policy="explicit")

    # Publish a message
    await jetstream.publish("test.ack", b"test message")

    # Fetch the message
    batch = await consumer.fetch(max_messages=1, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    msg = messages[0]

    # Acknowledge the message
    await msg.ack()

    # Verify the message was acknowledged by fetching again (should be empty)
    batch = await consumer.fetch(max_messages=1, max_wait=0.5)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 0


@pytest.mark.asyncio
async def test_message_double_ack_waits_for_server_confirmation(jetstream: JetStream):
    """Test double acknowledgment with server confirmation."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test-double-ack-stream",
        subjects=["test.double_ack"],
    )

    # Create a consumer
    consumer = await stream.create_consumer(name="test-double-ack-consumer", ack_policy="explicit")

    # Publish a message
    await jetstream.publish("test.double_ack", b"test message")

    # Fetch the message
    batch = await consumer.fetch(max_messages=1, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    msg = messages[0]

    # Double acknowledge the message with a timeout
    await msg.double_ack(timeout=2.0)

    # Verify the message was acknowledged
    batch = await consumer.fetch(max_messages=1, max_wait=0.5)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 0


@pytest.mark.asyncio
async def test_message_nak_triggers_redelivery(jetstream: JetStream):
    """Test negative acknowledgment."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test-nak-stream",
        subjects=["test.nak"],
    )

    # Create a consumer
    consumer = await stream.create_consumer(name="test-nak-consumer", ack_policy="explicit")

    # Publish a message
    await jetstream.publish("test.nak", b"test message")

    # Fetch the message
    batch = await consumer.fetch(max_messages=1, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    msg = messages[0]
    original_delivery_count = msg.metadata.num_delivered

    # Nak the message
    await msg.nak()

    # Give server time to process
    await asyncio.sleep(0.1)

    # Fetch again - should get the same message with increased delivery count
    batch = await consumer.fetch(max_messages=1, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 1
    redelivered_msg = messages[0]
    assert redelivered_msg.data == b"test message"
    assert redelivered_msg.metadata.num_delivered > original_delivery_count


@pytest.mark.asyncio
async def test_message_nak_with_delay_redelivers_after_delay(jetstream: JetStream):
    """Test negative acknowledgment with delay."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test-nak-delay-stream",
        subjects=["test.nak_delay"],
    )

    # Create a consumer
    consumer = await stream.create_consumer(name="test-nak-delay-consumer", ack_policy="explicit")

    # Publish a message
    await jetstream.publish("test.nak_delay", b"test message")

    # Fetch the message
    batch = await consumer.fetch(max_messages=1, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    msg = messages[0]

    # Nak with delay
    delay = 1.0  # 1 second delay
    await msg.nak_with_delay(delay)

    # Immediately try to fetch - should not get message
    batch = await consumer.fetch(max_messages=1, max_wait=0.5)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 0

    # Wait for the delay and fetch again
    await asyncio.sleep(delay + 0.2)
    batch = await consumer.fetch(max_messages=1, max_wait=0.5)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 1
    assert messages[0].data == b"test message"


@pytest.mark.asyncio
async def test_message_in_progress_resets_ack_timer(jetstream: JetStream):
    """Test in-progress acknowledgment."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test-wip-stream",
        subjects=["test.wip"],
    )

    # Create a consumer with ack wait (in nanoseconds)
    consumer = await stream.create_consumer(
        name="test-wip-consumer",
        ack_policy="explicit",
        ack_wait=2_000_000_000,  # 2 seconds in nanoseconds
    )

    # Publish a message
    await jetstream.publish("test.wip", b"test message")

    # Fetch the message
    batch = await consumer.fetch(max_messages=1, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    msg = messages[0]

    # Send in-progress multiple times
    for _ in range(3):
        await asyncio.sleep(0.5)
        await msg.in_progress()

    # Message should still not be redelivered
    batch = await consumer.fetch(max_messages=1, max_wait=0.5)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 0

    # Finally ack the message
    await msg.ack()


@pytest.mark.asyncio
async def test_message_term_prevents_redelivery(jetstream: JetStream):
    """Test message termination."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test-term-stream",
        subjects=["test.term"],
    )

    # Create a consumer with max deliver
    consumer = await stream.create_consumer(
        name="test-term-consumer",
        ack_policy="explicit",
        max_deliver=5,
    )

    # Publish a message
    await jetstream.publish("test.term", b"test message")

    # Fetch the message
    batch = await consumer.fetch(max_messages=1, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    msg = messages[0]

    # Terminate the message
    await msg.term()

    # Message should not be redelivered
    batch = await consumer.fetch(max_messages=1, max_wait=0.5)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 0


@pytest.mark.asyncio
async def test_message_term_with_reason_includes_reason_in_advisory(jetstream: JetStream):
    """Test message termination with reason."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test-term-reason-stream",
        subjects=["test.term_reason"],
    )

    # Create a consumer
    consumer = await stream.create_consumer(
        name="test-term-reason-consumer",
        ack_policy="explicit",
        max_deliver=5,
    )

    # Publish a message
    await jetstream.publish("test.term_reason", b"test message")

    # Fetch the message
    batch = await consumer.fetch(max_messages=1, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    msg = messages[0]

    # Terminate with reason
    await msg.term_with_reason("Failed to process: invalid format")

    # Message should not be redelivered
    batch = await consumer.fetch(max_messages=1, max_wait=0.5)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 0


@pytest.mark.asyncio
async def test_ack_methods_raise_error_without_jetstream_reference(client: Client):
    """Test that ack methods raise ValueError when JetStream reference is missing."""
    # Create a message without JetStream reference
    msg = Message(
        data=b"test",
        subject="test",
        reply_to="$JS.ACK.stream.consumer.1.1.1.1234567890.0",
        headers=None,
        jetstream=None,
    )

    # These should raise ValueError
    with pytest.raises(ValueError, match="Cannot acknowledge message without JetStream reference"):
        await msg.ack()
    with pytest.raises(ValueError, match="Cannot acknowledge message without JetStream reference"):
        await msg.double_ack()
    with pytest.raises(ValueError, match="Cannot NAK message without JetStream reference"):
        await msg.nak()
    with pytest.raises(ValueError, match="Cannot NAK message without JetStream reference"):
        await msg.nak_with_delay(1.0)
    with pytest.raises(ValueError, match="Cannot send in-progress for message without JetStream reference"):
        await msg.in_progress()
    with pytest.raises(ValueError, match="Cannot terminate message without JetStream reference"):
        await msg.term()
    with pytest.raises(ValueError, match="Cannot terminate message without JetStream reference"):
        await msg.term_with_reason("test reason")


@pytest.mark.asyncio
async def test_ack_methods_raise_error_without_reply_to(jetstream: JetStream):
    """Test that ack methods raise ValueError when reply_to is missing."""
    # Create a message without reply_to
    msg = Message(
        data=b"test",
        subject="test",
        reply_to=None,
        headers=None,
        jetstream=jetstream,
    )

    # These should raise ValueError
    with pytest.raises(ValueError, match="Cannot acknowledge message without reply_to"):
        await msg.ack()
    with pytest.raises(ValueError, match="Cannot acknowledge message without reply_to"):
        await msg.double_ack()
    with pytest.raises(ValueError, match="Cannot NAK message without reply_to"):
        await msg.nak()
    with pytest.raises(ValueError, match="Cannot NAK message without reply_to"):
        await msg.nak_with_delay(1.0)
    with pytest.raises(ValueError, match="Cannot send in-progress for message without reply_to"):
        await msg.in_progress()
    with pytest.raises(ValueError, match="Cannot terminate message without reply_to"):
        await msg.term()
    with pytest.raises(ValueError, match="Cannot terminate message without reply_to"):
        await msg.term_with_reason("test reason")


@pytest.mark.asyncio
async def test_fetch_and_acknowledge_multiple_messages(jetstream: JetStream):
    """Test fetching and acknowledging multiple messages."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test-multi-stream",
        subjects=["test.multi"],
    )

    # Create a consumer
    consumer = await stream.create_consumer(name="test-multi-consumer", ack_policy="explicit")

    # Publish multiple messages
    for i in range(5):
        await jetstream.publish("test.multi", f"message {i}".encode())

    # Fetch multiple messages
    batch = await consumer.fetch(max_messages=5, max_wait=1.0)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 5

    # Check each message has correct metadata
    for i, msg in enumerate(messages):
        assert msg.data == f"message {i}".encode()
        assert msg.metadata.stream == "test-multi-stream"
        assert msg.metadata.consumer == "test-multi-consumer"
        assert msg.metadata.sequence.stream == i + 1

    # Acknowledge all messages
    for msg in messages:
        await msg.ack()

    # Verify all were acknowledged
    batch = await consumer.fetch(max_messages=1, max_wait=0.5)
    messages = []
    async for msg in batch:
        messages.append(msg)
    assert len(messages) == 0
