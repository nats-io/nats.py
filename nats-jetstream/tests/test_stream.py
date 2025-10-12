"""Tests for JetStream stream functionality."""

import asyncio

import pytest
from nats.jetstream import JetStream, StreamInfo, StreamMessage


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
async def test_create_stream_with_invalid_max_msgs(jetstream: JetStream):
    """Test that creating a stream with invalid max_msgs fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.create_stream(
            name="test",
            subjects=["FOO.*"],
            max_msgs=-2,  # Invalid value
        )


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
    await stream.create_consumer(name="test_consumer", durable_name="test_consumer")

    updated_consumer = await stream.update_consumer("test_consumer", max_deliver=20)
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
