"""Tests for JetStream consumer functionality."""

import asyncio

import pytest
from nats.jetstream import JetStream


@pytest.mark.asyncio
async def test_consumer_fetch(jetstream: JetStream):
    """Test fetching a batch of messages from a consumer."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_fetch", subjects=["FETCH.*"])

    # Publish some messages
    messages = []
    for i in range(10):
        message = f"message {i}".encode()
        _ack = await jetstream.publish(f"FETCH.{i}", message)
        messages.append((f"FETCH.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="fetch_consumer", durable_name="fetch_consumer", filter_subject="FETCH.*", deliver_policy="all"
    )

    # Fetch a batch of messages
    batch = await consumer.fetch(max_messages=5, max_wait=1.0)

    # Collect all messages from the batch
    received = []
    async for msg in batch:
        received.append((msg.subject, msg.data))
        # Acknowledge the message
        await msg.ack()

    # Verify we received the expected messages
    assert len(received) == 5
    for i in range(5):
        assert received[i] == messages[i]

    # Fetch another batch
    batch2 = await consumer.fetch(max_messages=5, max_wait=1.0)

    # Collect all messages from the second batch
    received2 = []
    async for msg in batch2:
        received2.append((msg.subject, msg.data))
        # Acknowledge the message
        await msg.ack()

    # Verify we received the remaining messages
    assert len(received2) == 5
    for i in range(5):
        assert received2[i] == messages[i + 5]


@pytest.mark.asyncio
async def test_consumer_fetch_with_max_wait(jetstream: JetStream):
    """Test fetching messages with max_wait timeout working properly."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_fetch_timeout", subjects=["TIMEOUT.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="timeout_consumer", durable_name="timeout_consumer", filter_subject="TIMEOUT.*", deliver_policy="all"
    )

    # Fetch a batch with a very short timeout (no messages in stream)
    batch = await consumer.fetch(max_messages=5, max_wait=0.5)

    # Should timeout without any messages
    received = []
    async for msg in batch:
        received.append(msg)

    # Verify we received no messages due to timeout
    assert len(received) == 0


@pytest.mark.asyncio
async def test_consumer_fetch_nowait(jetstream: JetStream):
    """Test fetching messages with nowait option enabled.

    This is equivalent to FetchNoWait in the Go client.
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_no_wait", subjects=["NOWAIT.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="no_wait_consumer", durable_name="no_wait_consumer", filter_subject="NOWAIT.*", deliver_policy="all"
    )

    # Fetch with nowait when no messages are available
    batch = await consumer.fetch_nowait(max_messages=5)

    # Should return immediately with no messages
    received = []
    async for msg in batch:
        received.append(msg)

    # Verify we received no messages
    assert len(received) == 0

    # Now publish some messages
    messages = []
    for i in range(3):
        message = f"nowait {i}".encode()
        _ack = await jetstream.publish(f"NOWAIT.{i}", message)
        messages.append((f"NOWAIT.{i}", message))

    # Fetch with nowait - should get available messages
    batch = await consumer.fetch_nowait(max_messages=5)

    # Collect all messages from the batch
    received = []
    async for msg in batch:
        received.append((msg.subject, msg.data))
        await msg.ack()

    # Verify we received only the available messages
    assert len(received) == 3
    for i in range(3):
        assert received[i] == messages[i]


@pytest.mark.asyncio
async def test_consumer_fetch_with_max_bytes(jetstream: JetStream):
    """Test fetching messages with max_bytes option."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_max_bytes", subjects=["MAXBYTES.*"])

    # Publish some messages with different sizes
    small_msg = b"small"  # 5 bytes
    large_msg = b"a" * 1000  # 1000 bytes

    await jetstream.publish("MAXBYTES.1", small_msg)
    await jetstream.publish("MAXBYTES.2", large_msg)
    await jetstream.publish("MAXBYTES.3", small_msg)

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="max_bytes_consumer", durable_name="max_bytes_consumer", filter_subject="MAXBYTES.*", deliver_policy="all"
    )

    # Fetch with max_bytes limiting to approximately 1 large message + 1 small message
    batch = await consumer.fetch(
        max_bytes=1010,  # Just enough for the large message and one small one
        max_wait=1.0,
    )

    # Collect all messages from the batch
    received = []
    async for msg in batch:
        received.append(msg.data)
        await msg.ack()

    # Verify we received at most 2 messages due to byte limit
    assert 1 <= len(received) <= 2

    # If we got 2 messages, verify they match what we expect
    if len(received) == 2:
        assert small_msg in received
        assert large_msg in received


@pytest.mark.asyncio
async def test_consumer_delete_during_fetch(jetstream: JetStream):
    """Test deleting a consumer while a fetch is in progress."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_delete", subjects=["DELETE.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="delete_consumer", durable_name="delete_consumer", filter_subject="DELETE.*", deliver_policy="all"
    )

    # Publish some messages first (like Go test does)
    for i in range(5):
        await jetstream.publish(f"DELETE.{i}", f"delete {i}".encode())

    # Start a fetch for more messages than exist
    batch = await consumer.fetch(max_messages=10, max_wait=5.0)

    # In another task, delete the consumer after a short delay
    async def delete_after_delay():
        await asyncio.sleep(0.1)
        await stream.delete_consumer(consumer.name)

    # Start the deletion task
    delete_task = asyncio.create_task(delete_after_delay())

    # Process messages - we should get the 5 messages, then consumer deleted error
    received = []
    async for msg in batch:
        received.append(msg)
        await msg.ack()

    # Should have received the 5 messages before consumer was deleted
    assert len(received) == 5

    # Check that the batch has the consumer deleted error
    from nats.jetstream import ConsumerDeletedError

    assert batch.error is not None
    assert isinstance(batch.error, ConsumerDeletedError)
    assert "consumer deleted" in str(batch.error).lower()

    # Make sure the delete task completes
    await delete_task


@pytest.mark.asyncio
async def test_consumer_next(jetstream: JetStream):
    """Test fetching single messages one by one (similar to Next in Go client)."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_next", subjects=["NEXT.*"])

    # Publish some messages
    messages = []
    for i in range(5):
        message = f"next {i}".encode()
        await jetstream.publish(f"NEXT.{i}", message)
        messages.append((f"NEXT.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="next_consumer", durable_name="next_consumer", filter_subject="NEXT.*", deliver_policy="all"
    )

    # Fetch messages one by one
    received = []
    for i in range(5):
        msg = await consumer.next()
        received.append((msg.subject, msg.data))
        await msg.ack()

    # Verify we received all expected messages
    assert len(received) == 5
    for i in range(5):
        assert received[i] == messages[i]

    # Attempt to fetch another message with a short timeout
    with pytest.raises(asyncio.TimeoutError):
        await consumer.next(max_wait=0.5)


@pytest.mark.asyncio
async def test_consumer_delete_during_next(jetstream: JetStream):
    """Test deleting a consumer while waiting for a message with next."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_delete_next", subjects=["DELNEXT.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="delete_next_consumer",
        durable_name="delete_next_consumer",
        filter_subject="DELNEXT.*",
        deliver_policy="all",
    )

    # In another task, delete the consumer after a short delay
    async def delete_after_delay():
        await asyncio.sleep(0.1)
        await stream.delete_consumer(consumer.name)

    # Start the deletion task
    delete_task = asyncio.create_task(delete_after_delay())

    # Try to get a next message with a longer timeout (should be interrupted by deletion)
    # since there are no messages, next() will wait until the consumer is deleted
    from nats.jetstream import ConsumerDeletedError

    with pytest.raises(ConsumerDeletedError) as excinfo:
        await consumer.next(max_wait=2.0)

    # Verify the exception is related to consumer deletion
    assert "consumer deleted" in str(excinfo.value).lower()

    # Make sure the delete task completes
    await delete_task


@pytest.mark.asyncio
async def test_fetch_single_messages_one_by_one(jetstream: JetStream):
    """Test fetching single messages one by one using fetch with batch=1."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_fetch_single", subjects=["SINGLE.*"])

    # Publish some messages
    messages = []
    for i in range(5):
        message = f"single {i}".encode()
        await jetstream.publish(f"SINGLE.{i}", message)
        messages.append((f"SINGLE.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="single_consumer", durable_name="single_consumer", filter_subject="SINGLE.*", deliver_policy="all"
    )

    # Fetch messages one by one using batch=1
    received = []
    for i in range(5):
        batch = await consumer.fetch(max_messages=1, max_wait=1.0)
        async for msg in batch:
            received.append((msg.subject, msg.data))
            await msg.ack()

    # Verify we received all expected messages
    assert len(received) == 5
    for i in range(5):
        assert received[i] == messages[i]


@pytest.mark.asyncio
async def test_consumer_messages_as_iterator(jetstream: JetStream):
    """Test using messages() method to get a message stream for async iteration."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_consume_iter", subjects=["CONSUMEITER.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="consume_iter_consumer",
        durable_name="consume_iter_consumer",
        filter_subject="CONSUMEITER.*",
        deliver_policy="all",
    )

    # Create a message stream for manual iteration
    message_stream = await consumer.messages(
        max_messages=3,
        max_wait=1.0,
    )

    # Publish some messages first
    messages = []
    for i in range(3):
        message = f"consume_iter {i}".encode()
        await jetstream.publish(f"CONSUMEITER.{i}", message)
        messages.append((f"CONSUMEITER.{i}", message))

    # Give some time for messages to be processed
    await asyncio.sleep(0.5)

    # Collect messages using the iterator
    received = []

    async def collect_messages():
        async for msg in message_stream:
            received.append((msg.subject, msg.data))
            await msg.ack()
            if len(received) >= 3:
                break

    # Start collecting in the background
    collect_task = asyncio.create_task(collect_messages())

    try:
        # Wait for collection to complete
        await asyncio.wait_for(collect_task, timeout=5.0)

        # Stop the message stream
        await message_stream.stop()

        # Verify we received all the messages
        assert len(received) == 3
        for i in range(3):
            assert received[i] == messages[i]

    finally:
        # Ensure cleanup
        if not collect_task.done():
            collect_task.cancel()

        await message_stream.stop()


@pytest.mark.asyncio
async def test_messages_rejects_both_max_messages_and_max_bytes(jetstream: JetStream):
    """Test ADR-37: messages() cannot accept both max_messages and max_bytes simultaneously."""
    stream = await jetstream.create_stream(name="test", subjects=["FOO.*"])
    consumer = await stream.create_consumer(name="test_consumer")

    # Should raise ValueError when both are specified
    with pytest.raises(ValueError, match="Cannot specify both max_messages and max_bytes simultaneously"):
        await consumer.messages(max_messages=10, max_bytes=1024)


@pytest.mark.asyncio
async def test_consumer_prioritized_fetch(jetstream: JetStream):
    """Test priority-based message consumption using fetch.

    Based on Go test: TestConsumerPrioritized - Fetch subtest.
    Validates that lower priority values receive messages first.
    Requires NATS server 2.12.0+
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_prioritized", subjects=["PRIORITY.*"])

    # Create a consumer with prioritized policy
    consumer = await stream.create_consumer(
        name="priority_consumer",
        durable_name="priority_consumer",
        filter_subject="PRIORITY.*",
        deliver_policy="all",
        ack_policy="explicit",
        priority_policy="prioritized",
        priority_groups=["A"],
    )

    # Start both fetch requests BEFORE publishing messages
    # This ensures both requests are waiting when messages arrive
    # Lower priority consumer (priority=1) requests 100 messages
    batch1_task = asyncio.create_task(consumer.fetch(max_messages=100, max_wait=5.0, priority_group="A", priority=1))

    # Higher priority consumer (priority=0) requests 75 messages
    batch2_task = asyncio.create_task(consumer.fetch(max_messages=75, max_wait=5.0, priority_group="A", priority=0))

    # Give both requests time to be sent to the server
    await asyncio.sleep(0.1)

    # Now publish 100 messages - they should be distributed by priority
    for i in range(100):
        await jetstream.publish("PRIORITY.test", f"message {i}".encode())

    # Wait for both batches to complete
    batch2 = await batch2_task
    batch1 = await batch1_task

    # Collect messages from higher priority consumer (should get 75)
    received2 = []
    async for msg in batch2:
        received2.append(msg.data)
        await msg.ack()

    # Collect messages from lower priority consumer (should get 25)
    received1 = []
    async for msg in batch1:
        received1.append(msg.data)
        await msg.ack()

    # Higher priority (0) should have received 75 messages
    assert len(received2) == 75
    # Lower priority (1) should have received remaining 25 messages
    assert len(received1) == 25


@pytest.mark.asyncio
async def test_consumer_prioritized_next(jetstream: JetStream):
    """Test priority-based message consumption using next() method.
    Requires NATS server 2.12.0+
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_prioritized_next", subjects=["PNEXT.*"])

    # Create a consumer with prioritized policy
    consumer = await stream.create_consumer(
        name="priority_next_consumer",
        durable_name="priority_next_consumer",
        filter_subject="PNEXT.*",
        deliver_policy="all",
        ack_policy="explicit",
        priority_policy="prioritized",
        priority_groups=["B"],
    )

    # Publish 10 messages
    for i in range(10):
        await jetstream.publish("PNEXT.test", f"next {i}".encode())

    # Higher priority consumer should get messages
    received_high = []
    for _ in range(7):
        msg = await consumer.next(max_wait=1.0, priority_group="B", priority=0)
        received_high.append(msg.data)
        await msg.ack()

    # Lower priority consumer should get remaining messages
    received_low = []
    for _ in range(3):
        msg = await consumer.next(max_wait=1.0, priority_group="B", priority=5)
        received_low.append(msg.data)
        await msg.ack()

    assert len(received_high) == 7
    assert len(received_low) == 3


@pytest.mark.asyncio
async def test_consumer_prioritized_messages_stream(jetstream: JetStream):
    """Test priority-based message consumption using messages() stream.

    Based on Go test: TestConsumerPrioritized - Messages subtest.
    Requires NATS server 2.12.0+
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_prioritized_stream", subjects=["PSTREAM.*"])

    # Create a consumer with prioritized policy
    consumer = await stream.create_consumer(
        name="priority_stream_consumer",
        durable_name="priority_stream_consumer",
        filter_subject="PSTREAM.*",
        deliver_policy="all",
        ack_policy="explicit",
        priority_policy="prioritized",
        priority_groups=["C"],
    )

    # Publish 50 messages
    for i in range(50):
        await jetstream.publish("PSTREAM.test", f"stream {i}".encode())

    # Create two message streams with different priorities
    stream1 = await consumer.messages(
        max_messages=10,
        max_wait=2.0,
        priority_group="C",
        priority=1,  # Lower priority
    )

    stream2 = await consumer.messages(
        max_messages=10,
        max_wait=2.0,
        priority_group="C",
        priority=0,  # Higher priority
    )

    try:
        # Higher priority stream should receive more messages
        received1 = []
        received2 = []

        async def collect1():
            async for msg in stream1:
                received1.append(msg.data)
                await msg.ack()
                if len(received1) >= 20:
                    break

        async def collect2():
            async for msg in stream2:
                received2.append(msg.data)
                await msg.ack()
                if len(received2) >= 30:
                    break

        # Run both collectors concurrently
        await asyncio.gather(
            asyncio.wait_for(collect1(), timeout=5.0),
            asyncio.wait_for(collect2(), timeout=5.0),
        )

        # Higher priority (0) should receive more messages than lower priority (1)
        # Exact distribution may vary, but higher priority should dominate
        assert len(received2) >= len(received1)

    finally:
        await stream1.stop()
        await stream2.stop()


@pytest.mark.asyncio
async def test_consumer_pause_resume(jetstream: JetStream):
    """Test pausing and resuming a consumer.

    Requires NATS server 2.11.0+
    """
    import time

    # Create a stream
    stream = await jetstream.create_stream(name="test_pause", subjects=["PAUSE.*"])

    # Create a consumer
    consumer = await stream.create_consumer(
        name="pause_consumer",
        durable_name="pause_consumer",
        filter_subject="PAUSE.*",
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Publish some messages
    for i in range(10):
        await jetstream.publish("PAUSE.test", f"message {i}".encode())

    # Fetch a few messages to verify consumer is working
    batch = await consumer.fetch(max_messages=3, max_wait=1.0)
    received = []
    async for msg in batch:
        received.append(msg.data)
        await msg.ack()
    assert len(received) == 3

    # Pause the consumer for 2 seconds
    pause_until = time.time() + 2.0
    await stream.pause_consumer("pause_consumer", pause_until)

    # Verify consumer info shows it's paused
    info = await stream.get_consumer_info("pause_consumer")
    assert info.paused is True
    assert info.pause_remaining is not None
    assert info.pause_remaining.total_seconds() > 0

    # Try to fetch messages while paused - should timeout/get no messages quickly
    batch = await consumer.fetch(max_messages=5, max_wait=0.5)
    received_while_paused = []
    async for msg in batch:
        received_while_paused.append(msg.data)
        await msg.ack()

    # Should get no messages while paused (or very few if there's timing issues)
    assert len(received_while_paused) == 0

    # Wait for pause to expire
    await asyncio.sleep(2.1)

    # Verify consumer is automatically unpaused
    # When unpaused, server may not return paused/pause_remaining fields at all
    info = await stream.get_consumer_info("pause_consumer")
    assert (
        info.paused is False
        or info.paused is None
        or (info.pause_remaining is not None and info.pause_remaining.total_seconds() == 0)
    )

    # Should be able to fetch messages again
    batch = await consumer.fetch(max_messages=5, max_wait=1.0)
    received_after = []
    async for msg in batch:
        received_after.append(msg.data)
        await msg.ack()
    assert len(received_after) > 0


@pytest.mark.asyncio
async def test_consumer_resume(jetstream: JetStream):
    """Test explicitly resuming a paused consumer.

    Requires NATS server 2.11.0+
    """
    import time

    # Create a stream
    stream = await jetstream.create_stream(name="test_resume", subjects=["RESUME.*"])

    # Create a consumer
    consumer = await stream.create_consumer(
        name="resume_consumer",
        durable_name="resume_consumer",
        filter_subject="RESUME.*",
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Publish some messages
    for i in range(10):
        await jetstream.publish("RESUME.test", f"message {i}".encode())

    # Pause the consumer for a long time (10 seconds)
    pause_until = time.time() + 10.0
    await stream.pause_consumer("resume_consumer", pause_until)

    # Verify it's paused
    info = await stream.get_consumer_info("resume_consumer")
    assert info.paused is True

    # Explicitly resume it
    await stream.resume_consumer("resume_consumer")

    # Verify it's resumed (when unpaused, server may not return the paused field)
    info = await stream.get_consumer_info("resume_consumer")
    assert info.paused is False or info.paused is None

    # Should be able to fetch messages
    batch = await consumer.fetch(max_messages=5, max_wait=1.0)
    received = []
    async for msg in batch:
        received.append(msg.data)
        await msg.ack()
    assert len(received) == 5


@pytest.mark.asyncio
async def test_consumer_pause_invalid_stream(jetstream: JetStream):
    """Test error handling when pausing consumer with invalid stream name.

    Requires NATS server 2.11.0+
    """
    import time

    from nats.jetstream import JetStreamError as ApiError

    # Create a stream
    stream = await jetstream.create_stream(name="test_pause_error", subjects=["ERROR.*"])

    # Try to pause a consumer on a non-existent consumer
    # Should raise an error
    pause_until = time.time() + 60.0

    with pytest.raises(ApiError) as exc_info:
        await stream.pause_consumer("nonexistent_consumer", pause_until)

    assert exc_info.value.code is not None


@pytest.mark.asyncio
async def test_consumer_resume_invalid_stream(jetstream: JetStream):
    """Test error handling when resuming consumer with invalid stream name.

    Requires NATS server 2.11.0+
    """
    from nats.jetstream import JetStreamError as ApiError

    # Create a stream
    stream = await jetstream.create_stream(name="test_resume_error", subjects=["RERROR.*"])

    # Try to resume a consumer on a non-existent consumer
    # Should raise an error
    with pytest.raises(ApiError) as exc_info:
        await stream.resume_consumer("nonexistent_consumer")

    assert exc_info.value.code is not None


@pytest.mark.asyncio
async def test_consumer_info_not_paused_initially(jetstream: JetStream):
    """Test that newly created consumers are not paused.

    Validates that ConsumerInfo shows correct initial pause state.
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_initial_pause_state", subjects=["INITIAL.*"])

    # Create a consumer
    await stream.create_consumer(
        name="initial_consumer",
        durable_name="initial_consumer",
        filter_subject="INITIAL.*",
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Get consumer info
    info = await stream.get_consumer_info("initial_consumer")

    # Verify new consumer is not paused
    # paused and pause_remaining may be None (not returned) or False/0
    assert info.paused is None or info.paused is False
    assert info.pause_remaining is None or info.pause_remaining == 0


@pytest.mark.asyncio
async def test_create_consumer_with_multiple_filter_subjects(jetstream: JetStream):
    """Test creating consumer with multiple filter subjects (ADR-34).

    Based on Go test: TestCreateConsumer - "with multiple filter subjects"
    Requires NATS server 2.10+
    """
    # Create a stream with wildcard subject
    stream = await jetstream.create_stream(name="test_multi_filter", subjects=["FOO.*"])

    # Create consumer with multiple filter subjects
    consumer = await stream.create_consumer(
        name="multi_filter_consumer",
        filter_subjects=["FOO.A", "FOO.B"],
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Verify consumer was created
    assert consumer is not None

    # Verify filter_subjects was persisted correctly
    info = await stream.get_consumer_info(consumer.name)
    assert info.config.filter_subjects == ["FOO.A", "FOO.B"]
    # filter_subject (singular) should not be set
    assert info.config.filter_subject is None

    # Publish messages to different subjects
    await jetstream.publish("FOO.A", b"message A1")
    await jetstream.publish("FOO.B", b"message B1")
    await jetstream.publish("FOO.C", b"message C1")  # Should NOT be received
    await jetstream.publish("FOO.A", b"message A2")

    # Fetch messages - should only get FOO.A and FOO.B
    batch = await consumer.fetch(max_messages=10, max_wait=1.0)
    received = []
    async for msg in batch:
        received.append((msg.subject, msg.data))
        await msg.ack()

    # Should have received 3 messages (A1, B1, A2) but not C1
    assert len(received) == 3
    subjects = [subj for subj, _ in received]
    assert "FOO.A" in subjects
    assert "FOO.B" in subjects
    assert "FOO.C" not in subjects


@pytest.mark.asyncio
async def test_create_consumer_with_overlapping_filter_subjects(jetstream: JetStream):
    """Test error when creating consumer with overlapping filter subjects (ADR-34).

    Based on Go test: TestCreateConsumer - "with multiple filter subjects, overlapping subjects"
    Server should reject overlapping subjects with error code 10136.
    """
    from nats.jetstream import JetStreamError as ApiError

    # Create a stream with wildcard subject
    stream = await jetstream.create_stream(name="test_overlap_filter", subjects=["FOO.*"])

    # Attempt to create consumer with overlapping filter subjects
    # FOO.* overlaps with FOO.B
    with pytest.raises(ApiError) as exc_info:
        await stream.create_consumer(
            name="overlap_consumer",
            filter_subjects=["FOO.*", "FOO.B"],
            deliver_policy="all",
            ack_policy="explicit",
        )

    # Verify error message mentions overlapping
    error = exc_info.value
    assert "overlap" in str(error).lower()


@pytest.mark.asyncio
async def test_create_consumer_with_both_filter_subject_and_filter_subjects(jetstream: JetStream):
    """Test error when both filter_subject and filter_subjects are provided (ADR-34).

    Based on Go test: TestCreateConsumer - "with multiple filter subjects and filter subject provided"
    Server should reject with error code 10134 (duplicate filter subjects).
    """
    from nats.jetstream import JetStreamError as ApiError

    # Create a stream with wildcard subject
    stream = await jetstream.create_stream(name="test_both_filters", subjects=["FOO.*"])

    # Attempt to create consumer with both filter_subject AND filter_subjects
    with pytest.raises(ApiError) as exc_info:
        await stream.create_consumer(
            name="both_filters_consumer",
            filter_subject="FOO.C",
            filter_subjects=["FOO.A", "FOO.B"],
            deliver_policy="all",
            ack_policy="explicit",
        )

    # Verify error message mentions the conflict
    error = exc_info.value
    error_str = str(error).lower()
    assert "filter" in error_str and ("both" in error_str or "duplicate" in error_str)


@pytest.mark.asyncio
async def test_create_consumer_with_empty_filter_in_filter_subjects(jetstream: JetStream):
    """Test error when filter_subjects contains empty string (ADR-34).

    Based on Go test: TestCreateConsumer - "with empty subject in FilterSubjects"
    Server should reject with error about empty filter.
    """
    from nats.jetstream import JetStreamError as ApiError

    # Create a stream with wildcard subject
    stream = await jetstream.create_stream(name="test_empty_filter", subjects=["FOO.*"])

    # Attempt to create consumer with empty string in filter_subjects
    with pytest.raises(ApiError) as exc_info:
        await stream.create_consumer(
            name="empty_filter_consumer",
            filter_subjects=["FOO.A", ""],
            deliver_policy="all",
            ack_policy="explicit",
        )

    # Verify error message mentions empty filter
    error = exc_info.value
    assert "empty" in str(error).lower() or "filter" in str(error).lower()


@pytest.mark.asyncio
async def test_update_consumer_with_filter_subjects(jetstream: JetStream):
    """Test updating consumer to add filter_subjects (ADR-34).

    Verifies that filter_subjects can be set when updating an existing consumer.
    """
    # Create a stream with wildcard subject
    stream = await jetstream.create_stream(name="test_update_filter", subjects=["BAR.*"])

    # Create consumer without filter_subjects
    consumer = await stream.create_consumer(
        name="update_filter_consumer",
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Verify initial state - no filters
    assert consumer.info.config.filter_subject is None
    assert consumer.info.config.filter_subjects is None or consumer.info.config.filter_subjects == []

    # Mutate the config and update consumer to add filter_subjects
    consumer.info.config.filter_subjects = ["BAR.A", "BAR.B"]
    updated_consumer = await stream.update_consumer(consumer.info.config)

    # Verify filter_subjects was set
    info = await stream.get_consumer_info(updated_consumer.name)
    assert info.config.filter_subjects == ["BAR.A", "BAR.B"]

    # Test that filtering works
    await jetstream.publish("BAR.A", b"message A")
    await jetstream.publish("BAR.C", b"message C")  # Should not be received
    await jetstream.publish("BAR.B", b"message B")

    batch = await updated_consumer.fetch(max_messages=10, max_wait=1.0)
    received = []
    async for msg in batch:
        received.append((msg.subject, msg.data))
        await msg.ack()

    # Should only receive BAR.A and BAR.B
    assert len(received) == 2
    subjects = [subj for subj, _ in received]
    assert "BAR.A" in subjects
    assert "BAR.B" in subjects
    assert "BAR.C" not in subjects


@pytest.mark.asyncio
async def test_messages_with_custom_thresholds(jetstream: JetStream):
    """Test messages() with configurable threshold_messages and threshold_bytes (ADR-37).

    Verifies that custom thresholds work instead of the default 50%.
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_thresholds", subjects=["THRESH.*"])

    # Create a consumer
    consumer = await stream.create_consumer(
        name="threshold_consumer",
        filter_subject="THRESH.*",
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Publish some messages
    for i in range(20):
        await jetstream.publish("THRESH.test", f"message {i}".encode())

    # Create message stream with custom thresholds
    # Request batches of 10 messages, but refill when < 8 messages remain (80% threshold instead of 50%)
    message_stream = await consumer.messages(
        max_messages=10,
        threshold_messages=8,  # Refill at 80% consumed instead of default 50%
        max_wait=2.0,
    )

    try:
        received = []
        async for msg in message_stream:
            received.append(msg.data)
            await msg.ack()
            if len(received) >= 20:
                break

        # Should have received all 20 messages
        assert len(received) == 20

    finally:
        await message_stream.stop()


@pytest.mark.asyncio
async def test_messages_with_threshold_bytes(jetstream: JetStream):
    """Test messages() with threshold_bytes parameter (ADR-37).

    Verifies that byte-based thresholds work for refilling.
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_thresh_bytes", subjects=["TBYTES.*"])

    # Create a consumer
    consumer = await stream.create_consumer(
        name="thresh_bytes_consumer",
        filter_subject="TBYTES.*",
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Publish 10 messages of ~100 bytes each
    for i in range(10):
        await jetstream.publish("TBYTES.test", b"x" * 100)

    # Create message stream with byte thresholds (ADR-37: only max_bytes, not max_messages)
    # Request up to 1000 bytes, refill when < 800 bytes remain (80% threshold)
    message_stream = await consumer.messages(
        max_bytes=1000,
        threshold_bytes=800,  # Refill when less than 800 bytes buffered
        max_wait=2.0,
    )

    try:
        received = []
        async for msg in message_stream:
            received.append(msg.data)
            await msg.ack()
            if len(received) >= 10:
                break

        # Should have received all 10 messages
        assert len(received) == 10

    finally:
        await message_stream.stop()
