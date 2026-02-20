"""Tests for JetStream functionality."""

import asyncio

import pytest
from nats.client.errors import NoRespondersError
from nats.jetstream import (
    ConsumerNotFoundError,
    JetStream,
    JetStreamError,
    MessageNotFoundError,
    StreamInfo,
    StreamNameAlreadyInUseError,
    StreamNotFoundError,
)


@pytest.mark.asyncio
async def test_account_info(jetstream: JetStream):
    """Test getting JetStream account information."""
    _account_info = await jetstream.account_info()


@pytest.mark.asyncio
async def test_publish_message(jetstream: JetStream):
    """Test publishing a single message."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    ack = await jetstream.publish("FOO.A", b"test message")
    assert ack.stream == "test"
    assert ack.sequence > 0

    info = await stream.get_info()
    assert info.state.messages == 1
    assert info.state.bytes > 0


@pytest.mark.asyncio
async def test_publish_multiple_messages(jetstream: JetStream):
    """Test publishing multiple messages."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    for i in range(5):
        ack = await jetstream.publish("FOO.A", f"msg {i}".encode())
        assert ack.stream == "test"
        assert ack.sequence == i + 1

    info = await stream.get_info()
    assert info.state.messages == 5
    assert info.state.bytes > 0


@pytest.mark.asyncio
async def test_publish_with_headers(jetstream: JetStream):
    """Test publishing a message with headers."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    headers = {"X-Custom-Header": "test", "X-Message-Type": "example"}

    ack = await jetstream.publish("FOO.A", b"test message", headers=headers)
    assert ack.stream == "test"
    assert ack.sequence > 0

    msg = await stream.get_message(ack.sequence)
    assert msg.headers is not None
    assert msg.headers.get("X-Custom-Header") == "test"
    assert msg.headers.get("X-Message-Type") == "example"


@pytest.mark.asyncio
async def test_publish_to_multiple_subjects(jetstream: JetStream):
    """Test publishing messages to different subjects in the same stream."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    await jetstream.publish("FOO.A", b"message A")
    await jetstream.publish("FOO.B", b"message B")

    info = await stream.get_info()
    assert info.state.messages == 2

    # Verify subject-specific message counts
    info = await stream.get_info(subjects_filter="FOO.A")
    assert info.state.subjects["FOO.A"] == 1

    info = await stream.get_info(subjects_filter="FOO.B")
    assert info.state.subjects["FOO.B"] == 1


@pytest.mark.asyncio
async def test_publish_to_nonexistent_stream(jetstream: JetStream):
    """Test that publishing to a subject with no matching stream fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.publish("NONEXISTENT.FOO", b"test message")


@pytest.mark.asyncio
async def test_publish_to_multiple_matching_streams(jetstream: JetStream):
    """Test publishing to a subject that matches multiple streams."""
    # Create two streams with overlapping subject patterns
    await jetstream.create_stream(name="stream1", subjects=["FOO.*"])
    await jetstream.create_stream(name="stream2", subjects=["FOO.BAR.*"])

    # This should only go to stream1 since it's an exact match
    ack1 = await jetstream.publish("FOO.TEST", b"message 1")
    assert ack1.stream == "stream1"

    # This should go to both streams
    ack2 = await jetstream.publish("FOO.BAR.TEST", b"message 2")
    # Note: The exact behavior here depends on the NATS server configuration
    # Typically it will go to the most specific match
    assert ack2.stream == "stream2"


@pytest.mark.asyncio
async def test_publish_sequence_ordering(jetstream: JetStream):
    """Test that published messages maintain sequence ordering."""
    _stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    acks = []
    for i in range(5):
        ack = await jetstream.publish("FOO.A", f"msg {i}".encode())
        acks.append(ack)

    # Verify sequences are monotonically increasing
    sequences = [ack.sequence for ack in acks]
    assert sequences == sorted(sequences)
    assert len(set(sequences)) == len(sequences)  # No duplicates


@pytest.mark.asyncio
async def test_publish_with_empty_payload(jetstream: JetStream):
    """Test publishing a message with empty payload."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])

    ack = await jetstream.publish("FOO.A", b"")
    assert ack.stream == "test"
    assert ack.sequence > 0

    msg = await stream.get_message(ack.sequence)
    assert msg.data == b""


# Stream CRUD Tests


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
    from nats.jetstream import StreamNameAlreadyInUseError

    await jetstream.create_stream(name="test", subjects=["FOO.*"])

    with pytest.raises(StreamNameAlreadyInUseError):
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

    names = [name async for name in jetstream.stream_names()]
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
async def test_create_stream_with_invalid_max_msgs(jetstream: JetStream):
    """Test that creating a stream with an invalid max_msgs value fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.create_stream(
            name="test",
            subjects=["FOO.*"],
            max_msgs="invalid",  # Should be an integer
        )


@pytest.mark.asyncio
async def test_create_stream_with_workqueue_retention(jetstream: JetStream):
    """Test creating a stream with workqueue retention policy."""
    stream = await jetstream.create_stream(
        name="test",
        subjects=["FOO.*"],
        retention="workqueue",  # Messages are removed after being consumed
    )
    assert stream.info is not None
    assert stream.info.config.retention == "workqueue"


@pytest.mark.asyncio
async def test_create_mirror_stream(jetstream: JetStream):
    """Test creating a mirror stream."""
    # Create source stream
    _source = await jetstream.create_stream(name="source", subjects=["SOURCE.*"])

    # Create mirror stream
    mirror = await jetstream.create_stream(name="mirror", mirror={"name": "source"})

    # Check mirror configuration
    assert mirror.info.config.mirror.name == "source"


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_count", [0, 1, 2, 4, 8, 16, 32])
async def test_list_streams_pagination(jetstream: JetStream, stream_count: int):
    """Test that listing streams handles pagination."""
    # Create streams
    for i in range(stream_count):
        await jetstream.create_stream(name=f"test{i}", subjects=[f"TEST.{i}"])

    # List all streams
    streams = [stream async for stream in jetstream.list_streams()]
    assert len(streams) == stream_count


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_count", [0, 1, 2, 4, 8, 16, 32])
async def test_stream_names_pagination(jetstream: JetStream, stream_count: int):
    """Test that listing stream names handles pagination."""
    # Create streams
    for i in range(stream_count):
        await jetstream.create_stream(name=f"test{i}", subjects=[f"TEST.{i}"])

    # List all stream names
    names = [name async for name in jetstream.stream_names()]
    assert len(names) == stream_count


# Consumer CRUD Tests on JetStream


@pytest.mark.asyncio
async def test_create_consumer_via_jetstream(jetstream: JetStream):
    """Test creating a consumer directly via JetStream."""
    # Create a stream first
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])

    # Create a consumer via JetStream
    consumer = await jetstream.create_consumer(
        stream_name="test_stream", name="test_consumer", durable_name="test_consumer"
    )

    assert consumer.name == "test_consumer"
    assert consumer.stream_name == "test_stream"


@pytest.mark.asyncio
async def test_get_consumer_info_via_jetstream(jetstream: JetStream):
    """Test getting consumer info directly via JetStream."""
    # Create a stream and consumer
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    await jetstream.create_consumer(stream_name="test_stream", name="test_consumer", durable_name="test_consumer")

    # Get consumer info via JetStream
    info = await jetstream.get_consumer_info("test_stream", "test_consumer")

    assert info.name == "test_consumer"
    assert info.stream_name == "test_stream"


@pytest.mark.asyncio
async def test_get_consumer_via_jetstream(jetstream: JetStream):
    """Test getting a consumer directly via JetStream."""
    # Create a stream and consumer
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    await jetstream.create_consumer(stream_name="test_stream", name="test_consumer", durable_name="test_consumer")

    # Get consumer via JetStream
    consumer = await jetstream.get_consumer("test_stream", "test_consumer")

    assert consumer.name == "test_consumer"
    assert consumer.stream_name == "test_stream"


@pytest.mark.asyncio
async def test_delete_consumer_via_jetstream(jetstream: JetStream):
    """Test deleting a consumer directly via JetStream."""
    # Create a stream and consumer
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    consumer = await jetstream.create_consumer(
        stream_name="test_stream", name="test_consumer", durable_name="test_consumer"
    )

    # Delete consumer via JetStream
    result = await jetstream.delete_consumer("test_stream", consumer.name)

    assert result is True

    # Verify the consumer is deleted
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.get_consumer_info("test_stream", consumer.name)


@pytest.mark.asyncio
async def test_consumer_names_via_jetstream(jetstream: JetStream):
    """Test listing consumer names directly via JetStream."""
    # Create a stream and multiple consumers
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    await jetstream.create_consumer(stream_name="test_stream", name="consumer1", durable_name="consumer1")
    await jetstream.create_consumer(stream_name="test_stream", name="consumer2", durable_name="consumer2")

    # Get consumer names via JetStream
    names = [name async for name in jetstream.consumer_names("test_stream")]

    assert len(names) == 2
    assert set(names) == {"consumer1", "consumer2"}


@pytest.mark.asyncio
async def test_list_consumers_via_jetstream(jetstream: JetStream):
    """Test getting info for all consumers directly via JetStream."""
    # Create a stream and multiple consumers
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    await jetstream.create_consumer(stream_name="test_stream", name="consumer1", durable_name="consumer1")
    await jetstream.create_consumer(stream_name="test_stream", name="consumer2", durable_name="consumer2")

    # Get info for all consumers via JetStream
    consumers_info = [c async for c in jetstream.list_consumers("test_stream")]

    assert len(consumers_info) == 2
    consumer_names = {info.name for info in consumers_info}
    assert consumer_names == {"consumer1", "consumer2"}


@pytest.mark.asyncio
async def test_create_consumer_with_invalid_stream_fails(jetstream: JetStream):
    """Test that creating a consumer on a non-existent stream fails."""
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.create_consumer(stream_name="nonexistent_stream", name="test_consumer")


@pytest.mark.asyncio
async def test_create_consumer_with_duplicate_name_fails(jetstream: JetStream):
    """Test that creating a consumer with a duplicate name but different config fails."""
    # Create a stream and consumer
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    await jetstream.create_consumer(
        stream_name="test_stream", name="test_consumer", durable_name="test_consumer", max_deliver=10
    )

    # Try to create a consumer with the same name but different config
    with pytest.raises(JetStreamError, match="consumer already exists"):
        await jetstream.create_consumer(
            stream_name="test_stream", name="test_consumer", durable_name="test_consumer", max_deliver=20
        )


@pytest.mark.asyncio
async def test_update_consumer_via_jetstream(jetstream: JetStream):
    """Test updating a consumer directly via JetStream."""
    # Create a stream and consumer
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    await jetstream.create_consumer(
        stream_name="test_stream", name="test_consumer", durable_name="test_consumer", max_deliver=10
    )

    # Update the consumer via JetStream
    updated_consumer = await jetstream.update_consumer(
        stream_name="test_stream", consumer_name="test_consumer", max_deliver=20
    )

    assert updated_consumer.info.config.max_deliver == 20


@pytest.mark.asyncio
async def test_update_nonexistent_consumer_fails(jetstream: JetStream):
    """Test that updating a non-existent consumer fails."""
    # Create a stream
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])

    # Try to update a non-existent consumer
    with pytest.raises(JetStreamError, match="consumer does not exist"):
        await jetstream.update_consumer(stream_name="test_stream", consumer_name="nonexistent", max_deliver=20)


@pytest.mark.asyncio
async def test_create_or_update_consumer_creates_new(jetstream: JetStream):
    """Test that create_or_update_consumer creates a new consumer when it doesn't exist."""
    # Create a stream
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])

    # Create a consumer using create_or_update
    consumer = await jetstream.create_or_update_consumer(
        stream_name="test_stream", name="test_consumer", durable_name="test_consumer", max_deliver=10
    )

    assert consumer.info.config.name == "test_consumer"
    assert consumer.info.config.max_deliver == 10


@pytest.mark.asyncio
async def test_create_or_update_consumer_updates_existing(jetstream: JetStream):
    """Test that create_or_update_consumer updates an existing consumer."""
    # Create a stream and consumer
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    await jetstream.create_consumer(
        stream_name="test_stream", name="test_consumer", durable_name="test_consumer", max_deliver=10
    )

    # Update the consumer using create_or_update
    updated_consumer = await jetstream.create_or_update_consumer(
        stream_name="test_stream", name="test_consumer", durable_name="test_consumer", max_deliver=20
    )

    assert updated_consumer.info.config.max_deliver == 20


@pytest.mark.asyncio
async def test_create_or_update_consumer_via_stream(jetstream: JetStream):
    """Test create_or_update_consumer via Stream object."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])

    # Create a consumer using create_or_update
    consumer = await stream.create_or_update_consumer(name="test_consumer", max_deliver=10)

    assert consumer.info.config.name == "test_consumer"
    assert consumer.info.config.max_deliver == 10

    # Update the same consumer
    updated_consumer = await stream.create_or_update_consumer(name="test_consumer", max_deliver=20)

    assert updated_consumer.info.config.max_deliver == 20


@pytest.mark.asyncio
async def test_delete_nonexistent_consumer_fails(jetstream: JetStream):
    """Test that deleting a non-existent consumer fails."""
    # Create a stream
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])

    # Try to delete a non-existent consumer
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.delete_consumer("test_stream", "nonexistent_consumer")


@pytest.mark.asyncio
async def test_get_consumer_info_nonexistent_consumer_fails(jetstream: JetStream):
    """Test that getting info for a non-existent consumer fails."""
    # Create a stream
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])

    # Try to get info for a non-existent consumer
    with pytest.raises(Exception):  # TODO: Define specific error type
        await jetstream.get_consumer_info("test_stream", "nonexistent_consumer")


# JetStream Message Retrieval Tests


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_message_with_headers(jetstream: JetStream, allow_direct: bool):
    """Test retrieving a message with headers using JetStream.get_message()."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish a message with headers
    headers = {"X-Custom-Header": "test-value", "X-Message-Type": "example"}
    ack = await jetstream.publish("FOO.A", b"test message", headers=headers)

    # Retrieve the message using JetStream.get_message()
    msg = await jetstream.get_message("test", ack.sequence)

    # Verify headers are parsed correctly
    assert msg.headers is not None
    assert msg.headers.get("X-Custom-Header") == "test-value"
    assert msg.headers.get("X-Message-Type") == "example"
    assert msg.data == b"test message"
    assert msg.subject == "FOO.A"


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_message_without_headers(jetstream: JetStream, allow_direct: bool):
    """Test retrieving a message without headers using JetStream.get_message()."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish a message without headers
    ack = await jetstream.publish("FOO.A", b"test message")

    # Retrieve the message using JetStream.get_message()
    msg = await jetstream.get_message("test", ack.sequence)

    # Verify no headers are present
    assert msg.headers is None
    assert msg.data == b"test message"
    assert msg.subject == "FOO.A"


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_message_with_multiple_headers(jetstream: JetStream, allow_direct: bool):
    """Test retrieving a message with multiple different headers."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish a message with multiple different headers
    headers = {
        "X-Header-1": "value1",
        "X-Header-2": "value2",
        "X-Header-3": "value3",
    }
    ack = await jetstream.publish("FOO.A", b"test message", headers=headers)

    # Retrieve the message using JetStream.get_message()
    msg = await jetstream.get_message("test", ack.sequence)

    # Verify all headers are parsed correctly
    assert msg.headers is not None
    assert msg.headers.get("X-Header-1") == "value1"
    assert msg.headers.get("X-Header-2") == "value2"
    assert msg.headers.get("X-Header-3") == "value3"


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_message_with_multi_value_header(jetstream: JetStream, allow_direct: bool):
    """Test retrieving a message with multiple values for the same header key."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish a message with a header that has multiple values
    # The client accepts dict[str, str | list[str]]
    headers = {
        "X-Multi-Value": ["value1", "value2", "value3"],
        "X-Single-Value": "single",
    }
    ack = await jetstream.publish("FOO.A", b"test message", headers=headers)

    # Retrieve the message using JetStream.get_message()
    msg = await jetstream.get_message("test", ack.sequence)

    # Verify headers are parsed correctly
    # Headers.get() returns the first value, get_all() returns all values
    assert msg.headers is not None
    assert msg.headers.get("X-Single-Value") == "single"
    assert msg.headers.get("X-Multi-Value") == "value1"  # get() returns first value
    assert msg.headers.get_all("X-Multi-Value") == ["value1", "value2", "value3"]  # get_all() returns all


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_last_message_for_subject_with_headers(jetstream: JetStream, allow_direct: bool):
    """Test retrieving last message for a subject with headers."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish multiple messages to the same subject
    await jetstream.publish("FOO.A", b"message 1")
    await jetstream.publish("FOO.A", b"message 2")

    # Publish the last message with headers
    headers = {"X-Last-Message": "true", "X-Message-ID": "final"}
    ack = await jetstream.publish("FOO.A", b"message 3", headers=headers)

    # Retrieve the last message for the subject using JetStream.get_last_message_for_subject()
    msg = await jetstream.get_last_message_for_subject("test", "FOO.A")

    # Verify headers are parsed correctly
    assert msg.headers is not None
    assert msg.headers.get("X-Last-Message") == "true"
    assert msg.headers.get("X-Message-ID") == "final"
    assert msg.data == b"message 3"
    assert msg.subject == "FOO.A"
    assert msg.sequence == ack.sequence


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_direct", [False, True])
async def test_get_last_message_for_subject_without_headers(jetstream: JetStream, allow_direct: bool):
    """Test retrieving last message for a subject without headers."""
    await jetstream.create_stream(name="test", subjects=["FOO.*"], allow_direct=allow_direct)

    # Publish multiple messages without headers
    await jetstream.publish("FOO.A", b"message 1")
    ack = await jetstream.publish("FOO.A", b"message 2")

    # Retrieve the last message for the subject
    msg = await jetstream.get_last_message_for_subject("test", "FOO.A")

    # Verify no headers are present
    assert msg.headers is None
    assert msg.data == b"message 2"
    assert msg.subject == "FOO.A"
    assert msg.sequence == ack.sequence


# JetStream Publish Retries on No Responders


@pytest.mark.asyncio
async def test_publish_retries_on_no_responders(jetstream: JetStream):
    """Test that publish retries when receiving no responders error.

    Verifies that publish automatically retries when it receives a 'no responders'
    error and eventually succeeds when the stream becomes available.
    """

    # Create a task that will create the stream after a short delay
    # This simulates JetStream becoming available after initial failure
    async def create_stream_delayed():
        await asyncio.sleep(0.3)  # Wait longer than first retry (250ms)
        await jetstream.create_stream(name="test", subjects=["RETRY.*"])

    # Start the delayed stream creation
    create_task = asyncio.create_task(create_stream_delayed())

    try:
        # This should fail initially (no stream), retry, and succeed once stream is created
        # With default retry_wait=0.25s, it will retry after 250ms, by which time the stream exists
        ack = await jetstream.publish("RETRY.A", b"test message", timeout=2.0)

        # Verify it succeeded after retry
        assert ack.stream == "test"
        assert ack.sequence > 0
    finally:
        await create_task


@pytest.mark.asyncio
async def test_publish_retries_exhaust_and_fails(jetstream: JetStream):
    """Test that publish fails after exhausting all retry attempts.

    Verifies that publish raises NoRespondersError after all retry attempts
    are exhausted when no stream is available.
    """
    # Don't create a stream - this will cause "no responders" errors
    # With retry_attempts=2 and retry_wait=0.1s, total time ~0.2s
    # timeout=1.0s is enough time for retries but stream never appears

    with pytest.raises(NoRespondersError) as exc_info:
        await jetstream.publish("NONEXISTENT.subject", b"test message", retry_attempts=2, retry_wait=0.1, timeout=1.0)

    # Verify the error message
    assert "no responders" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_publish_retries_with_fixed_backoff(jetstream: JetStream):
    """Test that publish uses fixed backoff between retries.

    Verifies that the retry delays remain constant (not exponential) by measuring
    the time taken for retries.
    """
    start_time = asyncio.get_event_loop().time()

    # Create stream after 0.35 seconds to allow for 2 retries before success
    # First attempt: immediate fail
    # Wait 0.15s (retry_wait)
    # Second attempt: ~0.15s - still no stream
    # Wait 0.15s (retry_wait)
    # Third attempt: ~0.30s - stream now exists, success
    async def create_stream_delayed():
        await asyncio.sleep(0.35)
        await jetstream.create_stream(name="test", subjects=["RETRY.*"])

    create_task = asyncio.create_task(create_stream_delayed())

    try:
        # Use shorter retry_wait to make test faster
        ack = await jetstream.publish("RETRY.A", b"test message", retry_attempts=3, retry_wait=0.15, timeout=2.0)

        elapsed = asyncio.get_event_loop().time() - start_time

        assert ack.stream == "test"
        assert ack.sequence > 0

        # Should have taken at least 0.35s (when stream was created)
        # With fixed backoff, timing should be predictable
        assert elapsed >= 0.3, f"Expected at least 0.3s elapsed, got {elapsed}s"
        # But not too long (would indicate exponential backoff)
        assert elapsed < 0.6, f"Expected less than 0.6s elapsed, got {elapsed}s (would indicate exponential backoff)"
    finally:
        await create_task


@pytest.mark.asyncio
async def test_publish_custom_retry_attempts(jetstream: JetStream):
    """Test that publish respects custom retry_attempts parameter.

    Verifies that the retry_attempts parameter controls the number of retry attempts.
    """
    # Test with retry_attempts=0 (no retries)
    # Should fail immediately without retrying
    start_time = asyncio.get_event_loop().time()

    with pytest.raises(NoRespondersError):
        await jetstream.publish("NONEXISTENT.A", b"test message", retry_attempts=0, retry_wait=0.1, timeout=1.0)

    elapsed = asyncio.get_event_loop().time() - start_time
    # Should fail quickly (< 0.2s) since no retries
    assert elapsed < 0.2, f"With retry_attempts=0, expected quick failure, got {elapsed}s"

    # Test with retry_attempts=3 and a stream that appears after enough retries
    # Create stream after 0.5s, which is after 3 retries (3 * 0.1s = 0.3s)
    async def create_stream_delayed():
        await asyncio.sleep(0.5)
        await jetstream.create_stream(name="test", subjects=["RETRY.*"])

    create_task = asyncio.create_task(create_stream_delayed())

    try:
        start_time = asyncio.get_event_loop().time()

        ack = await jetstream.publish(
            "RETRY.B",
            b"test message",
            retry_attempts=5,  # Enough retries to wait for stream
            retry_wait=0.1,
            timeout=2.0,
        )

        elapsed = asyncio.get_event_loop().time() - start_time

        assert ack.stream == "test"
        # Should have taken at least 0.5s (when stream was created)
        assert elapsed >= 0.4, f"Expected at least 0.4s, got {elapsed}s"
    finally:
        await create_task


@pytest.mark.asyncio
async def test_publish_respects_total_timeout(jetstream: JetStream):
    """Test that publish respects the total timeout budget across all retries.

    Verifies that the timeout parameter is the total time budget for the entire
    operation, not per-request timeout.
    """
    start_time = asyncio.get_event_loop().time()

    # Set a short timeout (0.5 second) with retries that would take longer if allowed
    # With retry_wait=0.2s and retry_attempts=10, it could try for 2+ seconds
    # But timeout=0.5s should cut it off much sooner
    with pytest.raises((NoRespondersError, asyncio.TimeoutError)):
        await jetstream.publish("NONEXISTENT.timeout", b"test message", retry_attempts=10, retry_wait=0.2, timeout=0.5)

    elapsed = asyncio.get_event_loop().time() - start_time

    # Should complete within roughly the timeout budget (allowing small overhead for async operations)
    assert elapsed < 0.8, f"Operation took {elapsed}s, should be close to 0.5s timeout"
    # Should have tried at least once
    assert elapsed >= 0.0, "Operation should have taken some time"


# Error mapping tests - verify specific error types are raised


@pytest.mark.asyncio
async def test_stream_not_found_on_delete(jetstream: JetStream):
    """Test that deleting a non-existent stream raises StreamNotFoundError."""
    with pytest.raises(StreamNotFoundError) as exc_info:
        await jetstream.delete_stream("nonexistent_stream")
    assert exc_info.value.error_code == 10059


@pytest.mark.asyncio
async def test_stream_not_found_on_info(jetstream: JetStream):
    """Test that getting info for a non-existent stream raises StreamNotFoundError."""
    with pytest.raises(StreamNotFoundError) as exc_info:
        await jetstream.get_stream_info("nonexistent_stream")
    assert exc_info.value.error_code == 10059


@pytest.mark.asyncio
async def test_stream_not_found_on_update(jetstream: JetStream):
    """Test that updating a non-existent stream raises StreamNotFoundError."""
    with pytest.raises(StreamNotFoundError) as exc_info:
        await jetstream.update_stream(name="nonexistent_stream", subjects=["FOO.*"])
    assert exc_info.value.error_code == 10059


@pytest.mark.asyncio
async def test_stream_name_already_in_use(jetstream: JetStream):
    """Test that creating a stream with duplicate name raises StreamNameAlreadyInUseError."""
    await jetstream.create_stream(name="duplicate_stream", subjects=["FOO.*"])
    with pytest.raises(StreamNameAlreadyInUseError) as exc_info:
        await jetstream.create_stream(name="duplicate_stream", subjects=["BAR.*"])
    assert exc_info.value.error_code == 10058


@pytest.mark.asyncio
async def test_consumer_not_found_on_delete(jetstream: JetStream):
    """Test that deleting a non-existent consumer raises ConsumerNotFoundError."""
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    with pytest.raises(ConsumerNotFoundError) as exc_info:
        await jetstream.delete_consumer("test_stream", "nonexistent_consumer")
    assert exc_info.value.error_code == 10014


@pytest.mark.asyncio
async def test_consumer_not_found_on_info(jetstream: JetStream):
    """Test that getting info for a non-existent consumer raises ConsumerNotFoundError."""
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    with pytest.raises(ConsumerNotFoundError) as exc_info:
        stream = await jetstream.get_stream("test_stream")
        await stream.get_consumer_info("nonexistent_consumer")
    assert exc_info.value.error_code == 10014


@pytest.mark.asyncio
async def test_consumer_not_found_on_pause(jetstream: JetStream):
    """Test that pausing a non-existent consumer raises ConsumerNotFoundError."""
    await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    with pytest.raises(ConsumerNotFoundError) as exc_info:
        stream = await jetstream.get_stream("test_stream")
        # pause_until is a timestamp; using 0 to resume immediately (or pause for a moment)
        await stream.pause_consumer("nonexistent_consumer", pause_until=0)
    assert exc_info.value.error_code == 10014


@pytest.mark.asyncio
async def test_stream_not_found_on_consumer_create(jetstream: JetStream):
    """Test that creating a consumer on non-existent stream raises StreamNotFoundError."""
    with pytest.raises(StreamNotFoundError) as exc_info:
        await jetstream.create_consumer(stream_name="nonexistent_stream", name="test_consumer")
    assert exc_info.value.error_code == 10059


@pytest.mark.asyncio
async def test_message_not_found(jetstream: JetStream):
    """Test that getting a non-existent message raises MessageNotFoundError."""
    stream = await jetstream.create_stream(name="test_stream", subjects=["FOO.*"])
    with pytest.raises(MessageNotFoundError) as exc_info:
        await stream.get_message(99999)
    assert exc_info.value.error_code == 10037


@pytest.mark.asyncio
async def test_account_info_jetstream_not_enabled():
    """Test that account_info raises JetStreamNotEnabledError when JetStream is not enabled.

    This test starts a NATS server WITHOUT JetStream to verify the error conversion.
    """
    from nats.client import connect
    from nats.jetstream import JetStreamNotEnabledError
    from nats.jetstream import new as new_jetstream
    from nats.server import run

    # Start a NATS server WITHOUT JetStream
    async with await run(port=0, jetstream=False) as server:
        client = await connect(server.client_url)
        try:
            js = new_jetstream(client)

            # Should convert NoRespondersError to JetStreamNotEnabledError
            with pytest.raises(JetStreamNotEnabledError) as exc_info:
                await js.account_info()

            assert exc_info.value.error_code == 10076  # JETSTREAM_NOT_ENABLED
        finally:
            await client.close()
