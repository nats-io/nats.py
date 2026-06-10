"""Tests for JetStream stream functionality."""

import asyncio

import pytest
from nats.jetstream import (
    JetStream,
    JetStreamError,
    MessageNotFoundError,
    StreamConsumerSource,
    StreamInfo,
    StreamMessage,
    StreamSource,
)
from nats.jetstream.errors import ErrorCode
from nats.jetstream.headers import (
    NATS_BATCH_COMMIT,
    NATS_BATCH_COMMIT_EOB,
    NATS_BATCH_COMMIT_FINAL,
    NATS_BATCH_ID,
    NATS_BATCH_SEQUENCE,
    NATS_SCHEDULE,
    NATS_SCHEDULE_TARGET,
)


async def collect_async_iter(async_iter):
    """Helper function to collect async iterator results."""
    return [x async for x in async_iter]


@pytest.mark.asyncio
async def test_create_stream_with_subjects_and_description(jetstream: JetStream):
    """Test creating a stream with subjects and description."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], description="test stream")

    assert stream.name == "test"
    assert stream.info is not None
    assert stream.info.config.subjects == ["FOO.*"]
    assert stream.info.config.description == "test stream"


@pytest.mark.asyncio
async def test_create_stream_with_metadata(jetstream: JetStream):
    """Test creating a stream with metadata."""
    stream = await jetstream.create_stream(
        name="test_meta", subjects=["BAR.*"], metadata={"foo": "bar", "name": "test"}
    )
    assert stream.name == "test_meta"
    assert stream.info is not None
    assert stream.info.config.metadata is not None

    # Check that metadata contains at least the expected fields (server may add additional fields starting with _)
    expected_metadata = {"foo": "bar", "name": "test"}
    for key, value in expected_metadata.items():
        assert stream.info.config.metadata[key] == value


@pytest.mark.asyncio
async def test_create_stream_with_invalid_name_fails(jetstream: JetStream):
    """Test that creating a stream with an invalid name fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.create_stream(name="test.123", subjects=["FOO.*"])


@pytest.mark.asyncio
async def test_create_stream_with_empty_name_fails(jetstream: JetStream):
    """Test that creating a stream with an empty name fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.create_stream(name="", subjects=["FOO.*"])


@pytest.mark.asyncio
async def test_create_stream_with_duplicate_name_fails(jetstream: JetStream):
    """Test that creating a stream with a duplicate name fails."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"])

    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.create_stream(name="test", subjects=["BAR.*"])


@pytest.mark.asyncio
async def test_update_stream_max_msgs_and_subjects(jetstream: JetStream):
    """Test updating a stream's max messages and subjects."""
    # Create initial stream
    _stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], max_msgs=100)

    # Update stream configuration
    updated_info = await jetstream.update_stream(name="test", subjects=["FOO.*", "BAR.*"], max_msgs=200)
    assert updated_info.config.max_msgs == 200
    assert set(updated_info.config.subjects) == {"FOO.*", "BAR.*"}


@pytest.mark.asyncio
async def test_create_or_update_stream_creates_when_absent(jetstream: JetStream):
    """create_or_update_stream creates the stream when it does not exist."""
    stream = await jetstream.create_or_update_stream(name="test", subjects=["FOO.*"])
    assert stream.name == "test"
    info = await jetstream.get_stream_info("test")
    assert info.config.subjects == ["FOO.*"]


@pytest.mark.asyncio
async def test_create_or_update_stream_updates_when_present(jetstream: JetStream):
    """create_or_update_stream updates an existing stream in place (idempotent)."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"], max_msgs=100)
    stream = await jetstream.create_or_update_stream(name="test", subjects=["FOO.*", "BAR.*"], max_msgs=200)
    assert stream.name == "test"
    assert stream.info.config.subjects == ["FOO.*", "BAR.*"]
    assert stream.info.config.max_msgs == 200


@pytest.mark.asyncio
async def test_create_or_update_stream_accepts_config_object(jetstream: JetStream):
    """create_or_update_stream accepts a StreamConfig positionally, like create_stream."""
    from nats.jetstream import StreamConfig

    stream = await jetstream.create_or_update_stream(StreamConfig(name="test", subjects=["FOO.*"]))
    assert stream.name == "test"


@pytest.mark.asyncio
async def test_create_or_update_stream_requires_name(jetstream: JetStream):
    """create_or_update_stream raises ValueError when no name is provided."""
    with pytest.raises(ValueError):
        await jetstream.create_or_update_stream(subjects=["FOO.*"])


@pytest.mark.asyncio
async def test_create_or_update_stream_resolves_create_race(jetstream: JetStream):
    """Losing a create race to a concurrent caller still resolves idempotently.

    Simulates the race window by having the first update report the stream
    as missing while it actually exists: the fallback create then collides
    on the server and must recover with a final update."""
    from nats.jetstream import StreamNotFoundError

    await jetstream.create_stream(name="test", subjects=["FOO.*"])

    real_update = jetstream._api.stream_update
    update_calls = 0

    async def update_reporting_missing_once(name, /, **kwargs):
        nonlocal update_calls
        update_calls += 1
        if update_calls == 1:
            raise StreamNotFoundError("stream not found")
        return await real_update(name, **kwargs)

    jetstream._api.stream_update = update_reporting_missing_once

    stream = await jetstream.create_or_update_stream(name="test", subjects=["FOO.*", "BAR.*"])
    assert update_calls == 2
    assert set(stream.info.config.subjects) == {"FOO.*", "BAR.*"}


@pytest.mark.asyncio
async def test_update_nonexistent_stream_fails(jetstream: JetStream):
    """Test that updating a non-existent stream fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.update_stream(name="nonexistent", subjects=["FOO.*"])


@pytest.mark.asyncio
async def test_update_stream_with_invalid_name_fails(jetstream: JetStream):
    """Test that updating a stream with an invalid name fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.update_stream(name="test.123", subjects=["FOO.*"])


@pytest.mark.asyncio
async def test_delete_stream_succeeds(jetstream: JetStream):
    """Test successfully deleting a stream."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"])
    assert await jetstream.delete_stream("test") is True


@pytest.mark.asyncio
async def test_delete_nonexistent_stream_fails(jetstream: JetStream):
    """Test that deleting a non-existent stream fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.delete_stream("nonexistent")


@pytest.mark.asyncio
async def test_delete_stream_with_invalid_name_fails(jetstream: JetStream):
    """Test that deleting a stream with an invalid name fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.delete_stream("test.123")


@pytest.mark.asyncio
async def test_get_stream_info_returns_correct_data(jetstream: JetStream):
    """Test that getting stream info returns the correct data."""
    # Create a stream and publish messages
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    for i in range(5):
        await jetstream.publish("FOO.A", f"msg {i}".encode())
        await jetstream.publish("FOO.B", f"msg {i}".encode())

    info = await stream.get_info()
    assert isinstance(info, StreamInfo)
    assert info.config.name == "test"
    assert info.state.messages == 10
    assert info.state.consumer_count == 0


@pytest.mark.asyncio
async def test_get_stream_info_with_subject_filter(jetstream: JetStream):
    """Test getting stream info with a subject filter."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    for i in range(5):
        await jetstream.publish("FOO.A", f"msg {i}".encode())
        await jetstream.publish("FOO.B", f"msg {i}".encode())

    info = await stream.get_info(subjects_filter="FOO.A")
    assert info.state.subjects["FOO.A"] == 5


@pytest.mark.asyncio
async def test_get_nonexistent_stream_info_fails(jetstream: JetStream):
    """Test that getting info for a non-existent stream fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.get_stream_info("nonexistent")


@pytest.mark.asyncio
async def test_list_stream_names(jetstream: JetStream):
    """Test listing all stream names."""
    # Create multiple streams
    await jetstream.create_stream(name="test1", subjects=["FOO.*"])
    await jetstream.create_stream(name="test2", subjects=["BAR.*"])
    await jetstream.create_stream(name="test3", subjects=["BAZ.*"])

    names = await collect_async_iter(jetstream.stream_names())
    assert len(names) == 3
    assert set(names) == {"test1", "test2", "test3"}


@pytest.mark.asyncio
async def test_stream_names_iteration(jetstream: JetStream):
    """Test iterating over stream names."""
    await jetstream.create_stream(name="test1", subjects=["FOO.*"])
    await jetstream.create_stream(name="test2", subjects=["BAR.*"])
    await jetstream.create_stream(name="test3", subjects=["BAZ.*"])

    count = 0
    async for name in jetstream.stream_names():
        assert name in {"test1", "test2", "test3"}
        count += 1
    assert count == 3


@pytest.mark.asyncio
async def test_list_streams_info(jetstream: JetStream):
    """Test listing info for all streams."""
    await jetstream.create_stream(name="test1", subjects=["FOO.*"])
    await jetstream.create_stream(name="test2", subjects=["BAR.*"])

    streams = [stream async for stream in jetstream.list_streams()]
    assert len(streams) == 2
    assert {s.config.name for s in streams} == {"test1", "test2"}


@pytest.mark.asyncio
async def test_list_streams_with_subject_filter(jetstream: JetStream):
    """Test listing streams with a subject filter."""
    await jetstream.create_stream(name="test1", subjects=["FOO.*"])
    await jetstream.create_stream(name="test2", subjects=["BAR.*"])

    streams = [stream async for stream in jetstream.list_streams(subject="FOO.*")]
    assert len(streams) == 1
    assert streams[0].config.name == "test1"


@pytest.mark.asyncio
async def test_list_streams_empty(jetstream: JetStream):
    """Test listing streams when none exist."""
    streams = [stream async for stream in jetstream.list_streams()]
    assert len(streams) == 0


@pytest.mark.asyncio
async def test_list_streams_iteration(jetstream: JetStream):
    """Test iterating over streams one at a time."""
    await jetstream.create_stream(name="test1", subjects=["FOO.*"])
    await jetstream.create_stream(name="test2", subjects=["BAR.*"])

    count = 0
    async for stream in jetstream.list_streams():
        assert stream.config.name in {"test1", "test2"}
        assert isinstance(stream, StreamInfo)
        count += 1
    assert count == 2


@pytest.mark.asyncio
async def test_purge_all_messages(jetstream: JetStream):
    """Test purging all messages from a stream."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    for i in range(5):
        await jetstream.publish("FOO.A", f"msg {i} on A".encode())
        await jetstream.publish("FOO.B", f"msg {i} on B".encode())

    await stream.purge()
    info = await stream.get_info()
    assert info.state.messages == 0


@pytest.mark.asyncio
async def test_purge_with_subject_filter(jetstream: JetStream):
    """Test purging messages matching a subject filter."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    for i in range(5):
        await jetstream.publish("FOO.A", f"msg {i} on A".encode())
        await jetstream.publish("FOO.B", f"msg {i} on B".encode())

    await stream.purge(filter="FOO.A")
    info = await stream.get_info()
    assert info.state.messages == 5  # Only FOO.B messages remain


@pytest.mark.asyncio
async def test_purge_with_sequence(jetstream: JetStream):
    """Test purging messages up to a sequence number."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    for i in range(5):
        await jetstream.publish("FOO.A", f"msg {i}".encode())

    await stream.purge(sequence=3)
    info = await stream.get_info()
    assert info.state.messages == 3  # Only messages with seq >= 3 remain


@pytest.mark.asyncio
async def test_purge_with_keep(jetstream: JetStream):
    """Test purging messages while keeping a specified number."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    for i in range(10):
        await jetstream.publish("FOO.A", f"msg {i}".encode())

    await stream.purge(keep=3)
    info = await stream.get_info()
    assert info.state.messages == 3  # Keep the 3 most recent messages


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_message_by_sequence(jetstream: JetStream, allow_direct: bool):
    """Test getting a message by sequence number."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish a few messages
    messages = [f"msg {i}".encode() for i in range(5)]
    for i, msg in enumerate(messages):
        await jetstream.publish("FOO.A", msg)

    # Get a message by sequence
    msg = await stream.get_message(3)
    assert isinstance(msg, StreamMessage)
    assert msg.sequence == 3
    assert msg.data == messages[2]  # 0-indexed messages, 1-indexed sequences


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_message_not_found(jetstream: JetStream, allow_direct: bool):
    """Test that get_message raises MessageNotFoundError for both direct and non-direct paths."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    with pytest.raises(MessageNotFoundError):
        await stream.get_message(999)


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_last_message_for_subject_not_found(jetstream: JetStream, allow_direct: bool):
    """Test that get_last_message_for_subject raises MessageNotFoundError for both direct and non-direct paths."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    with pytest.raises(MessageNotFoundError):
        await stream.get_last_message_for_subject("FOO.NONE")


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_last_message_for_subject(jetstream: JetStream, allow_direct: bool):
    """Test getting the last message for a subject."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish a few messages
    for i in range(5):
        await jetstream.publish("FOO.A", f"msg {i} on A".encode())
        await jetstream.publish("FOO.B", f"msg {i} on B".encode())

    # Get the last message for a subject
    msg = await stream.get_last_message_for_subject("FOO.A")
    assert isinstance(msg, StreamMessage)
    assert msg.subject == "FOO.A"
    assert msg.data == b"msg 4 on A"


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_stream_get_message_with_headers(jetstream: JetStream, allow_direct: bool):
    """Test Stream.get_message() with headers for both direct and non-direct paths."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish a message with headers
    headers = {"X-Custom-Header": "test-value", "X-Message-Type": "example"}
    ack = await jetstream.publish("FOO.A", b"test message", headers=headers)

    # Retrieve the message using Stream.get_message()
    msg = await stream.get_message(ack.sequence)

    # Verify headers are parsed correctly
    assert msg.headers is not None
    assert msg.headers.get("X-Custom-Header") == "test-value"
    assert msg.headers.get("X-Message-Type") == "example"
    assert msg.data == b"test message"
    assert msg.subject == "FOO.A"


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_stream_get_last_message_for_subject_with_headers(jetstream: JetStream, allow_direct: bool):
    """Test Stream.get_last_message_for_subject() with headers for both direct and non-direct paths."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish multiple messages
    await jetstream.publish("FOO.A", b"message 1")
    await jetstream.publish("FOO.A", b"message 2")

    # Publish the last message with headers
    headers = {"X-Last-Message": "true", "X-Message-ID": "final"}
    ack = await jetstream.publish("FOO.A", b"message 3", headers=headers)

    # Retrieve the last message
    msg = await stream.get_last_message_for_subject("FOO.A")

    # Verify headers are parsed correctly
    assert msg.headers is not None
    assert msg.headers.get("X-Last-Message") == "true"
    assert msg.headers.get("X-Message-ID") == "final"
    assert msg.data == b"message 3"
    assert msg.subject == "FOO.A"
    assert msg.sequence == ack.sequence


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_stream_get_message_with_multi_value_header(jetstream: JetStream, allow_direct: bool):
    """Test Stream.get_message() with multiple values for the same header key."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish a message with a header that has multiple values
    headers = {
        "X-Multi-Value": ["value1", "value2", "value3"],
        "X-Single-Value": "single",
    }
    ack = await jetstream.publish("FOO.A", b"test message", headers=headers)

    # Retrieve the message using Stream.get_message()
    msg = await stream.get_message(ack.sequence)

    # Verify headers are parsed correctly
    # Headers.get() returns the first value, get_all() returns all values
    assert msg.headers is not None
    assert msg.headers.get("X-Single-Value") == "single"
    assert msg.headers.get("X-Multi-Value") == "value1"  # get() returns first value
    assert msg.headers.get_all("X-Multi-Value") == ["value1", "value2", "value3"]  # get_all() returns all


@pytest.mark.asyncio
async def test_delete_message(jetstream: JetStream):
    """Test deleting a message."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    # Publish a message
    await jetstream.publish("FOO.A", b"test message")

    # Delete the message
    await stream.delete_message(1)

    # Message should be gone
    with pytest.raises(Exception):  # TODO: Define specific error type
        await stream.get_message(1)


@pytest.mark.asyncio
async def test_secure_delete_message(jetstream: JetStream):
    """Test securely deleting a message."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    # Publish a message
    await jetstream.publish("FOO.A", b"sensitive data")

    # Securely delete the message (overwrites data)
    await stream.secure_delete_message(1)

    # Message should be gone
    with pytest.raises(Exception):  # TODO: Define specific error type
        await stream.get_message(1)


@pytest.mark.asyncio
async def test_stream_initial_state(jetstream: JetStream):
    """Test initial stream state after creation."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    info = await stream.get_info()
    assert info.state.messages == 0
    assert info.state.bytes == 0
    assert info.state.first_sequence == 0
    assert info.state.last_sequence == 0
    assert info.state.consumer_count == 0


@pytest.mark.asyncio
async def test_stream_state_after_messages(jetstream: JetStream):
    """Test stream state after publishing messages."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    for i in range(5):
        await jetstream.publish("FOO.A", f"msg {i}".encode())

    info = await stream.get_info()
    assert info.state.messages == 5
    assert info.state.bytes > 0
    assert info.state.first_sequence == 1
    assert info.state.last_sequence == 5


@pytest.mark.asyncio
async def test_create_stream_with_workqueue_retention(jetstream: JetStream):
    """Test creating a stream with workqueue retention policy."""
    stream = await jetstream.create_stream(
        name="test",
        subjects=["FOO.*"],
        retention="workqueue",  # Messages are removed after being consumed
    )
    assert stream.info.config.retention == "workqueue"


@pytest.mark.asyncio
async def test_create_mirror_stream(jetstream: JetStream):
    """Test creating a mirror stream."""
    # Create source stream
    _source = await jetstream.create_stream(name="source", subjects=["SOURCE.*"])

    # Create mirror stream
    mirror = await jetstream.create_stream(name="mirror", mirror={"name": "source"})

    # Publish to source
    await jetstream.publish("SOURCE.A", b"test message")

    # Wait a bit for the mirror to sync
    await asyncio.sleep(0.5)

    # Verify message appears in mirror
    mirror_info = await mirror.get_info()
    assert mirror_info.state.messages == 1
    assert mirror_info.mirror is not None
    assert mirror_info.mirror.name == "source"


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_count", [0, 1, 2, 4, 8, 16, 32, 64, 128, 255, 256, 257, 300, 512])
async def test_list_streams_pagination(jetstream: JetStream, stream_count: int):
    """Test that stream listing handles pagination correctly with different stream counts.

    Args:
        stream_count: Number of streams to create and test with
    """
    # Create streams
    stream_names = {f"test{i:03d}" for i in range(stream_count)}
    for name in stream_names:
        await jetstream.create_stream(name=name, subjects=[f"TEST.{name}.*"])

    # Collect all streams and verify we got them all
    streams = [stream async for stream in jetstream.list_streams()]
    assert len(streams) == stream_count
    assert {s.config.name for s in streams} == stream_names


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_count", [0, 1, 2, 4, 8, 16, 32, 64, 128, 255, 256, 257, 300, 512])
async def test_stream_names_pagination(jetstream: JetStream, stream_count: int):
    """Test that stream names listing handles pagination correctly with different stream counts.

    Args:
        stream_count: Number of streams to create and test with
    """
    # Create streams
    stream_names = {f"test{i:03d}" for i in range(stream_count)}
    for name in stream_names:
        await jetstream.create_stream(name=name, subjects=[f"TEST.{name}.*"])

    # Collect all names and verify we got them all
    names = await collect_async_iter(jetstream.stream_names())
    assert len(names) == stream_count
    assert set(names) == stream_names


# Consumer CRUD Tests


@pytest.mark.asyncio
async def test_create_consumer(jetstream: JetStream):
    """Test creating a consumer."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])
    consumer = await stream.create_consumer(name="test_consumer")
    assert consumer.name == "test_consumer"


@pytest.mark.asyncio
async def test_update_consumer(jetstream: JetStream):
    """Test updating a consumer's configuration."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])
    consumer = await stream.create_consumer(name="test_consumer", durable_name="test_consumer")

    # Mutate the config and update
    consumer.info.config.max_deliver = 20
    updated_consumer = await stream.update_consumer(consumer.info.config)
    assert updated_consumer.info.config.max_deliver == 20


@pytest.mark.asyncio
async def test_get_consumer_info(jetstream: JetStream):
    """Test retrieving consumer information."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])
    await stream.create_consumer(name="test_consumer", durable_name="test_consumer")

    info = await stream.get_consumer_info("test_consumer")
    assert info.name == "test_consumer"


@pytest.mark.asyncio
async def test_get_consumer(jetstream: JetStream):
    """Test retrieving a consumer instance."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])
    await stream.create_consumer(name="test_consumer", durable_name="test_consumer")

    consumer = await stream.get_consumer("test_consumer")
    assert consumer.name == "test_consumer"


@pytest.mark.asyncio
async def test_delete_consumer(jetstream: JetStream):
    """Test deleting a consumer."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])
    consumer = await stream.create_consumer(name="test_consumer", durable_name="test_consumer")

    assert await stream.delete_consumer(consumer.name) is True


@pytest.mark.asyncio
async def test_stream_republish(jetstream: JetStream):
    """Test ADR-28: Creating a stream with republish configuration."""
    from nats.jetstream.stream import Republish

    # Create stream with republish config
    # Note: dest must not overlap with stream subjects to avoid reingestion
    stream = await jetstream.create_stream(
        name="test_republish",
        subjects=["orders.*"],
        republish=Republish(
            src=">",  # Republish all messages
            dest="processed.>",  # Different subject space
        ),
    )

    assert stream.info.config.republish is not None
    assert stream.info.config.republish.src == ">"
    assert stream.info.config.republish.dest == "processed.>"
    assert stream.info.config.republish.headers_only is None


@pytest.mark.asyncio
async def test_stream_republish_headers_only(jetstream: JetStream):
    """Test ADR-28: Creating a stream with republish headers_only option."""
    from nats.jetstream.stream import Republish

    # Create stream with headers_only republish
    stream = await jetstream.create_stream(
        name="test_republish_headers",
        subjects=["events.*"],
        republish=Republish(src=">", dest="headers.>", headers_only=True),
    )

    assert stream.info.config.republish is not None
    assert stream.info.config.republish.headers_only is True


@pytest.mark.asyncio
async def test_stream_subject_transform(jetstream: JetStream):
    """Test ADR-36: Creating a stream with subject transform configuration."""
    from nats.jetstream.stream import SubjectTransform

    # Create stream with subject transform
    stream = await jetstream.create_stream(
        name="test_transform",
        subjects=["data.*"],
        subject_transform=SubjectTransform(src="data.>", dest="transformed.>"),
    )

    assert stream.info.config.subject_transform is not None
    assert stream.info.config.subject_transform.src == "data.>"
    assert stream.info.config.subject_transform.dest == "transformed.>"


@pytest.mark.asyncio
async def test_consumer_metadata(jetstream: JetStream):
    """Test ADR-33: Creating a consumer with metadata."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    # Create consumer with metadata
    consumer = await stream.create_consumer(
        name="test_consumer",
        metadata={"env": "test", "version": "1.0"},
    )

    assert consumer.info.config.metadata is not None
    # Check that metadata contains at least the expected fields (server may add additional fields starting with _)
    assert consumer.info.config.metadata["env"] == "test"
    assert consumer.info.config.metadata["version"] == "1.0"


@pytest.mark.asyncio
async def test_stream_info_created(jetstream: JetStream):
    """Test that stream info created is a timezone-aware datetime."""
    from datetime import datetime

    stream = await jetstream.create_stream(name="test_s_created", subjects=["SCRE.*"])
    info = await stream.get_info()
    assert isinstance(info.created, datetime)
    assert info.created.tzinfo is not None


@pytest.mark.asyncio
async def test_stream_info_timestamp(jetstream: JetStream):
    """Test that stream info timestamp is a timezone-aware datetime."""
    from datetime import datetime

    stream = await jetstream.create_stream(name="test_s_ts", subjects=["STS.*"])
    info = await stream.get_info()
    assert isinstance(info.timestamp, datetime)
    assert info.timestamp.tzinfo is not None


@pytest.mark.asyncio
async def test_stream_state_timestamps(jetstream: JetStream):
    """Test that stream state first_ts and last_ts are timezone-aware datetimes."""
    from datetime import datetime

    stream = await jetstream.create_stream(name="test_s_state_ts", subjects=["SSTS.*"])
    await jetstream.publish("SSTS.test", b"hello")

    info = await stream.get_info()
    assert isinstance(info.state.first_ts, datetime)
    assert info.state.first_ts.tzinfo is not None
    assert isinstance(info.state.last_ts, datetime)
    assert info.state.last_ts.tzinfo is not None


@pytest.mark.asyncio
async def test_stream_allow_msg_counter_round_trip(jetstream: JetStream):
    """Round-trip allow_msg_counter through stream create/info (ADR-49).

    Requires nats-server 2.12+.
    """
    stream = await jetstream.create_stream(
        name="COUNTER_RT",
        subjects=["counter.>"],
        allow_msg_counter=True,
    )

    info = await stream.get_info()
    assert info.config.allow_msg_counter is True


@pytest.mark.asyncio
async def test_publish_to_counter_stream(jetstream: JetStream):
    """Counter-enabled stream surfaces the running value on the publish ack (ADR-49).

    Requires nats-server 2.12+.
    """
    await jetstream.create_stream(
        name="COUNTER",
        subjects=["counter.>"],
        allow_msg_counter=True,
    )

    ack1 = await jetstream.publish("counter.x", b"", headers={"Nats-Incr": "5"})
    assert ack1.value == "5"

    ack2 = await jetstream.publish("counter.x", b"", headers={"Nats-Incr": "3"})
    assert ack2.value == "8"

    ack3 = await jetstream.publish("counter.x", b"", headers={"Nats-Incr": "-2"})
    assert ack3.value == "6"


@pytest.mark.asyncio
async def test_stream_persist_mode_round_trip(jetstream: JetStream):
    """Round-trip persist_mode through stream create/info (ADR-56).

    Requires nats-server 2.12+ (API Level 2). The server omits the field
    when persist_mode is the default, so we only round-trip ``"async"``.
    """
    stream = await jetstream.create_stream(
        name="ASYNC",
        subjects=["async.>"],
        num_replicas=1,
        persist_mode="async",
    )

    info = await stream.get_info()
    assert info.config.persist_mode == "async"


@pytest.mark.asyncio
async def test_stream_allow_msg_schedules_round_trip(jetstream: JetStream):
    """Round-trip allow_msg_schedules through stream create/info (ADR-51).

    Requires nats-server 2.14+.
    """
    stream = await jetstream.create_stream(
        name="SCHED_RT",
        subjects=["sched.>", "target.>"],
        allow_msg_schedules=True,
    )

    info = await stream.get_info()
    assert info.config.allow_msg_schedules is True


@pytest.mark.asyncio
async def test_publish_with_schedule_headers(jetstream: JetStream):
    """End-to-end: publish a scheduled message and verify the headers persist (ADR-51).

    Requires nats-server 2.14+.
    """
    stream = await jetstream.create_stream(
        name="SCHED",
        subjects=["sched.>", "target.>"],
        allow_msg_schedules=True,
    )

    ack = await jetstream.publish(
        "sched.every",
        b"",
        headers={NATS_SCHEDULE: "@every 5s", NATS_SCHEDULE_TARGET: "target.every"},
    )
    assert ack.sequence is not None

    msg = await stream.get_message(ack.sequence)
    assert msg.subject == "sched.every"
    assert msg.data == b""
    assert msg.headers is not None
    assert msg.headers.get(NATS_SCHEDULE) == "@every 5s"
    assert msg.headers.get(NATS_SCHEDULE_TARGET) == "target.every"


@pytest.mark.asyncio
async def test_publish_schedule_target_required(jetstream: JetStream):
    """Server rejects scheduled publish without a target (ADR-51).

    Requires nats-server 2.14+.
    """
    await jetstream.create_stream(
        name="SCHED_NOTARGET",
        subjects=["sched.>", "target.>"],
        allow_msg_schedules=True,
    )

    with pytest.raises(JetStreamError) as exc_info:
        await jetstream.publish(
            "sched.notarget",
            b"",
            headers={NATS_SCHEDULE: "@every 5s"},
        )

    assert exc_info.value.error_code == ErrorCode.SCHEDULE_TARGET_INVALID


@pytest.mark.asyncio
async def test_stream_allow_batch_publish_round_trip(jetstream: JetStream):
    """Round-trip allow_atomic / allow_batched flags through stream create/info (ADR-50).

    Requires nats-server 2.14+.
    """
    stream = await jetstream.create_stream(
        name="BATCH",
        subjects=["batch.>"],
        allow_atomic=True,
        allow_batched=True,
    )

    info = await stream.get_info()
    assert info.config.allow_atomic is True
    assert info.config.allow_batched is True


@pytest.mark.asyncio
async def test_atomic_batch_publish(jetstream: JetStream):
    """Atomic batch publish (ADR-50) returns batch_id/batch_size on the commit ack.

    Requires nats-server 2.14+.
    """
    stream = await jetstream.create_stream(
        name="BATCH_PUB",
        subjects=["bp.>"],
        allow_atomic=True,
    )

    batch_id = "batch-1"

    # ADR-50: the first batch message is sent as a request so the client can
    # detect feature support; the server replies with an empty body on success
    # (or an error). Subsequent pre-commit messages are fire-and-forget.
    handshake = await jetstream.client.request(
        "bp.a",
        b"one",
        headers={NATS_BATCH_ID: batch_id, NATS_BATCH_SEQUENCE: "1"},
        timeout=2.0,
    )
    assert handshake.data == b""

    await jetstream.client.publish(
        "bp.b",
        b"two",
        headers={NATS_BATCH_ID: batch_id, NATS_BATCH_SEQUENCE: "2"},
    )

    # The commit message gets a regular ack populated with batch/count.
    ack = await jetstream.publish(
        "bp.c",
        b"three",
        headers={
            NATS_BATCH_ID: batch_id,
            NATS_BATCH_SEQUENCE: "3",
            NATS_BATCH_COMMIT: NATS_BATCH_COMMIT_FINAL,
        },
        timeout=2.0,
    )

    assert ack.batch_id == batch_id
    assert ack.batch_size == 3

    # All three messages should be persisted on the stream.
    info = await stream.get_info()
    assert info.state.messages == 3


@pytest.mark.asyncio
async def test_atomic_batch_publish_eob_commit(jetstream: JetStream):
    """``Nats-Batch-Commit: eob`` commits without storing the final message (ADR-50).

    Requires nats-server 2.14+.
    """
    stream = await jetstream.create_stream(
        name="BATCH_EOB",
        subjects=["eb.>"],
        allow_atomic=True,
    )

    batch_id = "batch-eob"

    handshake = await jetstream.client.request(
        "eb.a",
        b"one",
        headers={NATS_BATCH_ID: batch_id, NATS_BATCH_SEQUENCE: "1"},
        timeout=2.0,
    )
    assert handshake.data == b""

    await jetstream.client.publish(
        "eb.b",
        b"two",
        headers={NATS_BATCH_ID: batch_id, NATS_BATCH_SEQUENCE: "2"},
    )

    # The EOB commit message itself is not persisted, only used to commit
    # the preceding messages in the batch.
    ack = await jetstream.publish(
        "eb.commit",
        b"",
        headers={
            NATS_BATCH_ID: batch_id,
            NATS_BATCH_SEQUENCE: "3",
            NATS_BATCH_COMMIT: NATS_BATCH_COMMIT_EOB,
        },
        timeout=2.0,
    )

    assert ack.batch_id == batch_id
    assert ack.batch_size == 2

    info = await stream.get_info()
    assert info.state.messages == 2


@pytest.mark.asyncio
async def test_stream_source_with_consumer(jetstream: JetStream):
    """Round-trip a Source(consumer=StreamConsumerSource(...)) through stream create/info (ADR-60).

    Does not exercise the actual sourcing flow: that requires binding to a push
    consumer, which the high-level client does not yet support.

    Requires nats-server 2.14+.
    """
    await jetstream.create_stream(name="UP", subjects=["up"])

    down = await jetstream.create_stream(
        name="DOWN",
        sources=[
            StreamSource(
                name="UP",
                consumer=StreamConsumerSource(name="C", deliver_subject="deliver"),
            ),
        ],
    )

    info = await down.get_info()
    assert info.config.sources is not None
    cs = info.config.sources[0].consumer
    assert cs is not None
    assert cs.name == "C"
    assert cs.deliver_subject == "deliver"
