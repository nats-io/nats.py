import asyncio
import uuid

import pytest
from nats.client import ClientStatus, connect
from nats.server import run


@pytest.mark.asyncio
async def test_subscription_receives_messages(client):
    """Test that a subscription receives messages published to its subject."""
    test_subject = f"test.{uuid.uuid4()}"
    test_message = b"Hello, NATS!"

    subscription = await client.subscribe(test_subject)
    await client.flush()  # Ensure subscription is registered

    await client.publish(test_subject, test_message)
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.data == test_message


@pytest.mark.asyncio
async def test_subscription_with_queue_receives_subset_of_messages_different_clients(server):
    """Test that subscriptions from different clients with queue group receives only a subset of messages."""
    # Create two clients
    client1 = await connect(server.client_url, timeout=1.0)
    client2 = await connect(server.client_url, timeout=1.0)

    try:
        test_subject = f"test.queue.{uuid.uuid4()}"
        queue = "test_queue"
        message_count = 20  # Send enough messages to ensure distribution

        # Set up subscriptions with the same queue group
        sub1 = await client1.subscribe(test_subject, queue=queue)
        sub2 = await client2.subscribe(test_subject, queue=queue)
        await client1.flush()
        await client2.flush()  # Ensure subscriptions are registered

        # Add small delay to ensure subscriptions are fully registered on server
        await asyncio.sleep(0.1)

        # Publish messages
        for i in range(message_count):
            await client1.publish(test_subject, f"Message {i}".encode())
        await client1.flush()

        # Count messages received by each subscription
        msg_count1 = 0
        msg_count2 = 0

        # Try to collect counts with timeout - use a longer timeout and better collection strategy
        # Collect all available messages with a reasonable timeout
        total_timeout = 3.0
        start_time = asyncio.get_event_loop().time()

        while (
            msg_count1 + msg_count2 < message_count and (asyncio.get_event_loop().time() - start_time) < total_timeout
        ):
            try:
                await asyncio.wait_for(sub1.next(), 0.1)
                msg_count1 += 1
            except asyncio.TimeoutError:
                pass

            try:
                await asyncio.wait_for(sub2.next(), 0.1)
                msg_count2 += 1
            except asyncio.TimeoutError:
                pass

        # Each subscription should receive fewer than all messages
        assert msg_count1 < message_count
        assert msg_count2 < message_count

        # But together they should receive most or all messages
        assert msg_count1 + msg_count2 >= message_count * 0.8
    finally:
        await client1.close()
        await client2.close()


@pytest.mark.asyncio
async def test_subscription_with_queue_receives_subset_of_messages_same_client(client):
    """Test that subscriptions from the same client with queue group receives only a subset of messages."""
    test_subject = f"test.queue_same_client.{uuid.uuid4()}"
    queue = "test_queue_same_client"
    message_count = 20  # Send enough messages to ensure distribution

    # Set up subscriptions with the same queue group from the same client
    sub1 = await client.subscribe(test_subject, queue=queue)
    sub2 = await client.subscribe(test_subject, queue=queue)
    await client.flush()  # Ensure subscriptions are registered

    # Add small delay to ensure subscriptions are fully registered on server
    await asyncio.sleep(0.1)

    # Publish messages
    for i in range(message_count):
        await client.publish(test_subject, f"Message {i}".encode())
    await client.flush()

    # Count messages received by each subscription
    msg_count1 = 0
    msg_count2 = 0

    # Try to collect counts with timeout - use a longer timeout and better collection strategy
    # Collect all available messages with a reasonable timeout
    total_timeout = 3.0
    start_time = asyncio.get_event_loop().time()

    while msg_count1 + msg_count2 < message_count and (asyncio.get_event_loop().time() - start_time) < total_timeout:
        try:
            await asyncio.wait_for(sub1.next(), 0.1)
            msg_count1 += 1
        except asyncio.TimeoutError:
            pass

        try:
            await asyncio.wait_for(sub2.next(), 0.1)
            msg_count2 += 1
        except asyncio.TimeoutError:
            pass

    # Each subscription should receive fewer than all messages
    assert msg_count1 < message_count
    assert msg_count2 < message_count

    # But together they should receive most or all messages
    assert msg_count1 + msg_count2 >= message_count * 0.8


@pytest.mark.asyncio
async def test_subscription_without_queue_receives_all_messages_different_clients(server):
    """Test that multiple subscriptions from different clients without queue groups each receive all messages."""
    # Create two clients
    client1 = await connect(server.client_url, timeout=1.0)
    client2 = await connect(server.client_url, timeout=1.0)

    try:
        test_subject = f"test.no_queue.{uuid.uuid4()}"
        message_count = 5

        # Set up subscriptions without queue group
        sub1 = await client1.subscribe(test_subject)
        sub2 = await client2.subscribe(test_subject)
        await client1.flush()
        await client2.flush()

        # Add small delay to ensure subscriptions are fully registered on server
        await asyncio.sleep(0.1)

        # Publish messages
        for i in range(message_count):
            await client1.publish(test_subject, f"Message {i}".encode())

        # Ensure all messages are published
        await client1.flush()

        # Collect all messages from both subscriptions
        messages1 = []
        messages2 = []

        # Collect messages with timeout
        try:
            for _ in range(message_count):
                message1 = await sub1.next(timeout=3.0)
                messages1.append(message1.data)
        except asyncio.TimeoutError:
            pass

        try:
            for _ in range(message_count):
                message2 = await sub2.next(timeout=3.0)
                messages2.append(message2.data)
        except asyncio.TimeoutError:
            pass

        # Both subscriptions should receive all messages
        assert len(messages1) == message_count, f"sub1 received {len(messages1)} messages, expected {message_count}"
        assert len(messages2) == message_count, f"sub2 received {len(messages2)} messages, expected {message_count}"

        # Both should receive the same set of messages (order may vary)
        assert set(messages1) == set(messages2)
    finally:
        await client1.close()
        await client2.close()


@pytest.mark.asyncio
async def test_subscription_without_queue_receives_all_messages_same_client(client):
    """Test that multiple subscriptions from the same client without queue groups each receive all messages."""
    test_subject = f"test.no_queue_same_client.{uuid.uuid4()}"
    message_count = 5

    # Set up two subscriptions from the same client without queue group
    sub1 = await client.subscribe(test_subject)
    sub2 = await client.subscribe(test_subject)
    await client.flush()

    # Add small delay to ensure subscriptions are fully registered on server
    await asyncio.sleep(0.1)

    # Publish messages
    for i in range(message_count):
        await client.publish(test_subject, f"Message {i}".encode())

    # Ensure all messages are published
    await client.flush()

    # Collect all messages from both subscriptions
    messages1 = []
    messages2 = []

    # Collect messages with timeout
    try:
        for _ in range(message_count):
            message1 = await sub1.next(timeout=3.0)
            messages1.append(message1.data)
    except asyncio.TimeoutError:
        pass

    try:
        for _ in range(message_count):
            message2 = await sub2.next(timeout=3.0)
            messages2.append(message2.data)
    except asyncio.TimeoutError:
        pass

    # Both subscriptions should receive all messages
    assert len(messages1) == message_count, f"sub1 received {len(messages1)} messages, expected {message_count}"
    assert len(messages2) == message_count, f"sub2 received {len(messages2)} messages, expected {message_count}"

    # Both should receive the same set of messages (order may vary)
    assert set(messages1) == set(messages2)


@pytest.mark.asyncio
async def test_subscription_star_wildcard_receives_matching_messages(client):
    """Test that a subscription with * wildcard receives messages for matching subjects."""
    # Create base subject and variants
    base = f"test.wild.{uuid.uuid4()}"
    subject1 = f"{base}.foo"
    subject2 = f"{base}.bar"
    subject3 = f"{base}.foo.bar"  # Should not match with *

    # Subscribe with * wildcard (matches single token)
    subscription = await client.subscribe(f"{base}.*")
    await client.flush()

    # Publish messages to different subjects
    await client.publish(subject1, b"Message 1")
    await client.publish(subject2, b"Message 2")
    await client.publish(subject3, b"Message 3")
    await client.flush()

    # Should receive messages for subject1 and subject2 only
    received_subjects = set()
    try:
        while True:
            message = await asyncio.wait_for(subscription.next(), 0.5)
            received_subjects.add(message.subject)
    except asyncio.TimeoutError:
        pass

    assert subject1 in received_subjects
    assert subject2 in received_subjects
    assert subject3 not in received_subjects
    assert len(received_subjects) == 2


@pytest.mark.asyncio
async def test_subscription_greater_than_wildcard_receives_all_matching(client):
    """Test that subscription with > wildcard receives all matching hierarchical messages."""
    # Create base subject and variants
    base = f"test.wild.{uuid.uuid4()}"
    subject1 = f"{base}.foo"
    subject2 = f"{base}.bar"
    subject3 = f"{base}.foo.bar"  # Should match with >

    # Subscribe with > wildcard (matches all remaining tokens)
    subscription = await client.subscribe(f"{base}.>")
    await client.flush()

    # Publish messages to different subjects
    await client.publish(subject1, b"Message 1")
    await client.publish(subject2, b"Message 2")
    await client.publish(subject3, b"Message 3")
    await client.flush()

    # Should receive all messages
    received_subjects = set()
    try:
        while True:
            message = await asyncio.wait_for(subscription.next(), 0.5)
            received_subjects.add(message.subject)
    except asyncio.TimeoutError:
        pass

    assert subject1 in received_subjects
    assert subject2 in received_subjects
    assert subject3 in received_subjects
    assert len(received_subjects) == 3


@pytest.mark.asyncio
async def test_subscription_next_with_timeout_raises_on_timeout(client):
    """Test that subscription.next() with timeout raises TimeoutError when no message received."""
    test_subject = f"test.timeout.{uuid.uuid4()}"

    subscription = await client.subscribe(test_subject)
    await client.flush()

    with pytest.raises(asyncio.TimeoutError):
        await subscription.next(timeout=0.2)


@pytest.mark.asyncio
async def test_subscription_unsubscribe_stops_receiving(client):
    """Test that unsubscribing stops receiving any further messages."""
    test_subject = f"test.unsub.{uuid.uuid4()}"

    # Create subscription
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish a message and verify it's received
    await client.publish(test_subject, b"Before unsubscribe")
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.data == b"Before unsubscribe"

    # Unsubscribe
    await subscription.unsubscribe()

    # Publish another message
    await client.publish(test_subject, b"After unsubscribe")
    await client.flush()

    # Verify the message is not received
    with pytest.raises(RuntimeError):
        await subscription.next(timeout=0.5)


@pytest.mark.asyncio
async def test_subscription_close_is_same_as_unsubscribe(client):
    """Test that closing a subscription is equivalent to unsubscribing."""
    test_subject = f"test.close.{uuid.uuid4()}"

    # Create subscription
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish a message and verify it's received
    await client.publish(test_subject, b"Before close")
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.data == b"Before close"

    # Unsubscribe from the subscription
    await subscription.unsubscribe()

    # Publish another message
    await client.publish(test_subject, b"After close")
    await client.flush()

    # Verify the message is not received
    with pytest.raises(RuntimeError):
        await subscription.next(timeout=0.5)


@pytest.mark.asyncio
async def test_subscription_as_context_manager(client):
    """Test that Subscription can be used as an async context manager."""
    test_subject = f"test.context.{uuid.uuid4()}"

    # Use subscription as context manager
    async with await client.subscribe(test_subject) as subscription:
        await client.flush()
        # Publish a message
        await client.publish(test_subject, b"Context test")
        await client.flush()

        # Verify message is received
        message = await subscription.next(timeout=1.0)
        assert message.data == b"Context test"

        # Subscription should still be active
        assert not subscription.closed

    # Subscription should be closed after context exit
    assert subscription.closed

    # Verify subscription is closed by attempting to receive
    with pytest.raises(RuntimeError):
        await subscription.next(timeout=0.5)


@pytest.mark.asyncio
async def test_client_close_also_closes_subscriptions(client):
    """Test that closing the client also closes all its subscriptions."""
    test_subject = f"test.client_close.{uuid.uuid4()}"

    # Create subscription
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Close the client
    await client.close()

    # Verify client status
    assert client.status == ClientStatus.CLOSED

    # Verify subscription is also closed
    with pytest.raises(RuntimeError):
        await subscription.next(timeout=0.5)


@pytest.mark.asyncio
async def test_subscription_receives_headers(client):
    """Test that a subscription receives headers in messages when the server supports them."""
    # Skip if headers not supported
    if not client.server_info or not client.server_info.headers:
        pytest.skip("Server does not support headers")

    test_subject = f"test.headers.{uuid.uuid4()}"
    header_key = "custom-header"
    header_value = "test-value"
    headers = {header_key: header_value}

    # Setup subscription
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish message with headers
    await client.publish(test_subject, b"Test", headers=headers)
    await client.flush()

    # Receive and verify
    message = await subscription.next(timeout=1.0)
    assert message.headers is not None
    assert message.headers.get(header_key) == header_value


@pytest.mark.asyncio
async def test_subscription_receives_messages_after_reconnection(server):
    """Test that a subscription continues to receive messages after reconnection."""
    # Create a client with reconnection enabled
    client = await connect(
        server.client_url,
        timeout=1.0,
        allow_reconnect=True,
        reconnect_max_attempts=10,
        reconnect_time_wait=1.0,
    )

    new_server = None
    try:
        test_subject = f"test.reconnect.{uuid.uuid4()}"
        test_message = b"Hello, NATS!"

        # Create subscription
        subscription = await client.subscribe(test_subject)
        await client.flush()

        # Publish and verify first message
        await client.publish(test_subject, test_message)
        await client.flush()
        message = await subscription.next(timeout=1.0)
        assert message.data == test_message

        # Store the server port before stopping it
        server_port = server.port

        # Stop the server to simulate connection loss
        await server.shutdown()

        # Create a new server on the same port
        new_server = await run(port=server_port)

        # Wait for client to reconnect
        max_wait = 5.0
        start_time = asyncio.get_event_loop().time()
        while client.status != ClientStatus.CONNECTED:
            if asyncio.get_event_loop().time() - start_time > max_wait:
                msg = "Client failed to reconnect within timeout"
                raise TimeoutError(msg)
            await asyncio.sleep(0.1)

        # Publish and verify second message
        await client.publish(test_subject, test_message)
        await client.flush()
        message = await subscription.next(timeout=1.0)
        assert message.data == test_message

    finally:
        # Clean up in reverse order of creation
        if client:
            await client.close()

        # Clean up the new server if it was created
        if new_server:
            await new_server.shutdown()


@pytest.mark.asyncio
async def test_subscription_multiple_callbacks(client):
    """Test that multiple callbacks can be added to a subscription and all are invoked."""
    test_subject = f"test.multiple_callbacks.{uuid.uuid4()}"
    test_message = b"Hello, multiple callbacks!"

    # Counters to track callback invocations
    callback1_count = 0
    callback2_count = 0
    callback3_count = 0

    received_messages = []

    def callback1(msg):
        nonlocal callback1_count
        callback1_count += 1
        received_messages.append(f"callback1: {msg.data}")

    def callback2(msg):
        nonlocal callback2_count
        callback2_count += 1
        received_messages.append(f"callback2: {msg.data}")

    def callback3(msg):
        nonlocal callback3_count
        callback3_count += 1
        received_messages.append(f"callback3: {msg.data}")

    # Create subscription and add multiple callbacks
    subscription = await client.subscribe(test_subject)
    subscription.add_callback(callback1)
    subscription.add_callback(callback2)
    subscription.add_callback(callback3)

    await client.flush()

    # Publish a message
    await client.publish(test_subject, test_message)
    await client.flush()

    # Give callbacks time to execute
    await asyncio.sleep(0.1)

    # Verify all callbacks were invoked
    assert callback1_count == 1, f"Expected callback1 to be called once, got {callback1_count}"
    assert callback2_count == 1, f"Expected callback2 to be called once, got {callback2_count}"
    assert callback3_count == 1, f"Expected callback3 to be called once, got {callback3_count}"

    # Verify messages were received by all callbacks
    assert len(received_messages) == 3
    assert f"callback1: {test_message}" in received_messages
    assert f"callback2: {test_message}" in received_messages
    assert f"callback3: {test_message}" in received_messages

    # Verify message is still available via next()
    message = await subscription.next(timeout=1.0)
    assert message.data == test_message


@pytest.mark.asyncio
async def test_subscription_remove_callback(client):
    """Test that callbacks can be removed from a subscription."""
    test_subject = f"test.remove_callback.{uuid.uuid4()}"
    test_message = b"Hello, remove callback!"

    # Counters to track callback invocations
    callback1_count = 0
    callback2_count = 0

    def callback1(_msg):
        nonlocal callback1_count
        callback1_count += 1

    def callback2(_msg):
        nonlocal callback2_count
        callback2_count += 1

    # Create subscription and add callbacks
    subscription = await client.subscribe(test_subject)
    subscription.add_callback(callback1)
    subscription.add_callback(callback2)

    await client.flush()

    # Publish first message
    await client.publish(test_subject, test_message)
    await client.flush()
    await asyncio.sleep(0.1)

    # Verify both callbacks were invoked
    assert callback1_count == 1
    assert callback2_count == 1

    # Remove callback1
    subscription.remove_callback(callback1)

    # Reset counters
    callback1_count = 0
    callback2_count = 0

    # Publish second message
    await client.publish(test_subject, test_message)
    await client.flush()
    await asyncio.sleep(0.1)

    # Verify only callback2 was invoked
    assert callback1_count == 0, f"Expected callback1 to not be called, got {callback1_count}"
    assert callback2_count == 1, f"Expected callback2 to be called once, got {callback2_count}"

    # Try to remove a callback that's not in the list (should not raise)
    def callback3(_msg):
        pass

    subscription.remove_callback(callback3)  # Should not raise


@pytest.mark.asyncio
async def test_subscription_callback_with_initial_callback(client):
    """Test that add_callback/remove_callback works correctly."""
    test_subject = f"test.initial_callback.{uuid.uuid4()}"
    test_message = b"Hello, initial callback!"

    # Counters to track callback invocations
    initial_callback_count = 0
    added_callback_count = 0

    def initial_callback(_msg):
        nonlocal initial_callback_count
        initial_callback_count += 1

    def added_callback(_msg):
        nonlocal added_callback_count
        added_callback_count += 1

    # Create subscription and add callbacks
    subscription = await client.subscribe(test_subject)
    subscription.add_callback(initial_callback)
    subscription.add_callback(added_callback)

    await client.flush()

    # Publish a message
    await client.publish(test_subject, test_message)
    await client.flush()
    await asyncio.sleep(0.1)

    # Verify both callbacks were invoked
    assert initial_callback_count == 1, f"Expected initial callback to be called once, got {initial_callback_count}"
    assert added_callback_count == 1, f"Expected added callback to be called once, got {added_callback_count}"

    # Remove the initial callback
    subscription.remove_callback(initial_callback)

    # Reset counters
    initial_callback_count = 0
    added_callback_count = 0

    # Publish second message
    await client.publish(test_subject, test_message)
    await client.flush()
    await asyncio.sleep(0.1)

    # Verify only added_callback was invoked
    assert initial_callback_count == 0, f"Expected initial callback to not be called, got {initial_callback_count}"
    assert added_callback_count == 1, f"Expected added callback to be called once, got {added_callback_count}"


@pytest.mark.asyncio
async def test_subscription_callbacks_with_headers(client):
    """Test that callbacks work correctly with messages that have headers."""
    test_subject = f"test.callbacks_headers.{uuid.uuid4()}"
    test_message = b"Hello, headers!"
    test_headers = {"X-Test": "value", "X-Count": "42"}

    received_messages = []

    def callback_with_headers(msg):
        # Convert headers to simple dict with single values
        headers_dict = None
        if msg.headers:
            headers_dict = {}
            for key, value_list in msg.headers.items():
                headers_dict[key] = value_list[0] if value_list else None

        received_messages.append({"data": msg.data, "headers": headers_dict, "subject": msg.subject})

    # Create subscription with callback
    subscription = await client.subscribe(test_subject)
    subscription.add_callback(callback_with_headers)

    await client.flush()

    # Publish message with headers
    await client.publish(test_subject, test_message, headers=test_headers)
    await client.flush()
    await asyncio.sleep(0.1)

    # Verify callback received message with headers
    assert len(received_messages) == 1
    received = received_messages[0]
    assert received["data"] == test_message
    assert received["subject"] == test_subject
    assert received["headers"] is not None
    assert received["headers"]["X-Test"] == "value"
    assert received["headers"]["X-Count"] == "42"


@pytest.mark.asyncio
async def test_subscription_callback_exception_handling(client):
    """Test that exceptions in callbacks don't break the subscription."""
    test_subject = f"test.callback_exception.{uuid.uuid4()}"
    test_message = b"Hello, exception handling!"

    # Counters to track callback invocations
    good_callback_count = 0
    bad_callback_count = 0

    def good_callback(_msg):
        nonlocal good_callback_count
        good_callback_count += 1

    def bad_callback(_msg):
        nonlocal bad_callback_count
        bad_callback_count += 1
        error_msg = "This callback always fails"
        raise ValueError(error_msg)

    # Create subscription with both good and bad callbacks
    subscription = await client.subscribe(test_subject)
    subscription.add_callback(good_callback)
    subscription.add_callback(bad_callback)

    await client.flush()

    # Publish a message
    await client.publish(test_subject, test_message)
    await client.flush()
    await asyncio.sleep(0.1)

    # Verify both callbacks were called despite the exception
    assert good_callback_count == 1, f"Expected good callback to be called once, got {good_callback_count}"
    assert bad_callback_count == 1, f"Expected bad callback to be called once, got {bad_callback_count}"

    # Verify message is still available via next() despite callback exception
    message = await subscription.next(timeout=1.0)
    assert message.data == test_message


@pytest.mark.asyncio
async def test_subscription_stops_iterating_on_close(client):
    """Test that async iterator stops when subscription is closed."""
    test_subject = f"test.iterator_close.{uuid.uuid4()}"

    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Unsubscribe from the subscription
    await subscription.unsubscribe()

    # Try to iterate - should stop immediately (StopAsyncIteration)
    messages_received = 0
    async for _msg in subscription:
        messages_received += 1

    # Should receive no messages since subscription is closed
    assert messages_received == 0


@pytest.mark.asyncio
async def test_subscription_drain_processes_pending_messages(client):
    """Test that drain allows pending messages to be processed."""
    test_subject = f"test.drain.{uuid.uuid4()}"

    # Subscribe
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish multiple messages
    for i in range(5):
        await client.publish(test_subject, f"message-{i}".encode())
    await client.flush()

    # Wait for one message to be received
    messages_received = []
    message = await subscription.next(timeout=0.5)
    messages_received.append(message.data.decode())

    # Drain the subscription (stops new messages, allows pending to be consumed)
    await subscription.drain()

    # We should still be able to read all pending messages
    try:
        while True:
            message = await subscription.next(timeout=0.5)
            messages_received.append(message.data.decode())
    except (RuntimeError, asyncio.TimeoutError):
        # Expected when queue is exhausted or closed
        pass

    # Verify we received all 5 messages
    assert len(messages_received) == 5
    assert messages_received == ["message-0", "message-1", "message-2", "message-3", "message-4"]

    # Verify subscription is closed
    assert subscription.closed

    # Publish another message - it should NOT be received since we drained
    await client.publish(test_subject, b"after-drain")
    await client.flush()

    # Try to get a message - should fail since subscription is closed
    with pytest.raises(RuntimeError, match="Subscription is closed"):
        await subscription.next(timeout=0.5)


@pytest.mark.asyncio
async def test_many_subscriptions_on_same_subject():
    """Test that client can handle many concurrent subscriptions on the same subject.

    This stress test verifies that the client can manage a large number of
    subscriptions all listening to the same subject, with each receiving all messages.
    """
    server = await run(port=0)

    try:
        client = await connect(server.client_url, timeout=1.0)

        try:
            num_subscriptions = 100
            test_subject = f"test.many.same.{uuid.uuid4()}"
            subscriptions = []

            # Create many subscriptions on the same subject
            for i in range(num_subscriptions):
                sub = await client.subscribe(test_subject)
                subscriptions.append(sub)

            await client.flush()

            # Publish a single message
            test_message = b"shared_message"
            await client.publish(test_subject, test_message)
            await client.flush()

            # Verify all subscriptions receive the message
            for i, sub in enumerate(subscriptions):
                msg = await sub.next(timeout=2.0)
                assert msg.data == test_message, f"Subscription {i} received wrong message"
                assert msg.subject == test_subject, f"Subscription {i} received wrong subject"

        finally:
            await client.close()

    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_many_subscriptions_on_unique_subjects():
    """Test that client can handle many concurrent subscriptions on unique subjects.

    This stress test verifies that the client can manage a large number of
    subscriptions simultaneously, each on a unique subject and receiving its own messages.
    """
    server = await run(port=0)

    try:
        client = await connect(server.client_url, timeout=1.0)

        try:
            num_subscriptions = 100
            subscriptions = []
            subjects = []

            # Create many subscriptions on unique subjects
            for i in range(num_subscriptions):
                subject = f"test.many.unique.{uuid.uuid4()}.{i}"
                subjects.append(subject)
                sub = await client.subscribe(subject)
                subscriptions.append(sub)

            await client.flush()

            # Publish a message to each unique subject
            for i, subject in enumerate(subjects):
                await client.publish(subject, f"msg_{i}".encode())

            await client.flush()

            # Verify each subscription receives its specific message
            for i, sub in enumerate(subscriptions):
                msg = await sub.next(timeout=2.0)
                assert msg.data == f"msg_{i}".encode(), f"Subscription {i} received wrong message"
                assert msg.subject == subjects[i], f"Subscription {i} received wrong subject"

        finally:
            await client.close()

    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_multiple_concurrent_consumers_using_next(client):
    """Test multiple tasks consuming from the same subscription using .next().

    This verifies that multiple concurrent consumers can safely read from the
    same subscription, with each message being delivered to exactly one consumer.
    This simulates real-world scenarios like worker pools processing messages.
    """
    test_subject = f"test.concurrent.next.{uuid.uuid4()}"
    message_count = 50

    # Create subscription
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Track messages received by each consumer
    consumer_messages = {0: [], 1: [], 2: []}

    async def consumer_task(consumer_id: int):
        """Consumer task that processes messages using .next().

        Simulates a worker that continuously processes messages from a queue.
        """
        while True:
            try:
                msg = await subscription.next(timeout=0.5)
                # Simulate some processing work
                await asyncio.sleep(0.01)
                consumer_messages[consumer_id].append(msg.data.decode())
            except asyncio.TimeoutError:
                # No more messages available - worker is done
                break
            except RuntimeError:
                # Subscription closed
                break

    # Start multiple concurrent consumer tasks (simulating a worker pool)
    num_consumers = 3
    consumer_tasks = [asyncio.create_task(consumer_task(i)) for i in range(num_consumers)]

    try:
        # Give consumers time to start waiting for work
        await asyncio.sleep(0.1)

        # Publish messages slowly to allow fair distribution across workers
        for i in range(message_count):
            await client.publish(test_subject, f"message_{i}".encode())
            if i % 10 == 0:
                await asyncio.sleep(0.01)  # Small delay to allow distribution
        await client.flush()

        # Wait for all consumer tasks to finish processing
        await asyncio.gather(*consumer_tasks, return_exceptions=True)

        # Verify all messages were received exactly once
        all_messages = []
        for messages in consumer_messages.values():
            all_messages.extend(messages)

        assert len(all_messages) == message_count, f"Expected {message_count} messages, got {len(all_messages)}"

        # Verify no duplicate messages
        assert len(set(all_messages)) == message_count, "Some messages were received multiple times"

    finally:
        # Ensure tasks are complete
        for task in consumer_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass


@pytest.mark.asyncio
async def test_multiple_concurrent_consumers_using_async_for(client):
    """Test multiple tasks consuming from the same subscription using async for.

    This verifies that multiple concurrent consumers using async iteration
    can safely read from the same subscription.
    This simulates real-world scenarios like event processors using async iteration.
    """
    test_subject = f"test.concurrent.iter.{uuid.uuid4()}"
    message_count = 50

    # Create subscription
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Track messages received by each consumer
    consumer_messages = {0: [], 1: [], 2: []}
    stop_event = asyncio.Event()

    async def consumer_task(consumer_id: int):
        """Consumer task that processes messages using async for iteration.

        Simulates an event processor that uses async iteration to handle messages.
        """
        async for msg in subscription:
            # Simulate some processing work
            await asyncio.sleep(0.01)
            consumer_messages[consumer_id].append(msg.data.decode())

            # Stop when we've received all expected messages across all consumers
            total = sum(len(messages) for messages in consumer_messages.values())
            if total >= message_count:
                break
            if stop_event.is_set():
                break

    # Start multiple concurrent consumer tasks (simulating event processors)
    num_consumers = 3
    consumer_tasks = [asyncio.create_task(consumer_task(i)) for i in range(num_consumers)]

    try:
        # Give consumers time to start their event loops
        await asyncio.sleep(0.1)

        # Publish messages slowly to allow fair distribution across processors
        for i in range(message_count):
            await client.publish(test_subject, f"message_{i}".encode())
            if i % 10 == 0:
                await asyncio.sleep(0.01)  # Small delay to allow distribution
        await client.flush()

        # Wait for all messages to be consumed (with timeout)
        max_wait = 5.0
        start = asyncio.get_event_loop().time()
        while sum(len(messages) for messages in consumer_messages.values()) < message_count:
            if asyncio.get_event_loop().time() - start > max_wait:
                break
            await asyncio.sleep(0.1)

        # Signal consumers to stop
        stop_event.set()
        await subscription.unsubscribe()

        # Wait for consumer tasks to finish
        await asyncio.wait_for(asyncio.gather(*consumer_tasks, return_exceptions=True), timeout=2.0)

        # Verify all messages were received
        all_messages = []
        for messages in consumer_messages.values():
            all_messages.extend(messages)

        assert len(all_messages) == message_count, f"Expected {message_count} messages, got {len(all_messages)}"

        # Verify no duplicate messages
        assert len(set(all_messages)) == message_count, "Some messages were received multiple times"

    finally:
        stop_event.set()
        for task in consumer_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass


@pytest.mark.asyncio
async def test_async_iteration_with_concurrent_publishers(client):
    """Test async iteration while multiple tasks are publishing concurrently.

    This verifies that async for iteration works correctly when messages are
    being published continuously by multiple publishers.
    This simulates real-world scenarios with multiple producers and a single consumer.
    """
    test_subject = f"test.iter.concurrent.pub.{uuid.uuid4()}"
    messages_per_publisher = 20
    num_publishers = 3

    # Create subscription
    subscription = await client.subscribe(test_subject)
    await client.flush()

    received_messages = []
    stop_iteration = asyncio.Event()

    async def consumer_task():
        """Consumer task using async for iteration.

        Simulates a single consumer processing events from multiple producers.
        """
        async for msg in subscription:
            # Simulate some processing work
            await asyncio.sleep(0.005)
            received_messages.append(msg.data.decode())
            if stop_iteration.is_set():
                break

    async def publisher_task(publisher_id: int):
        """Publisher task that continuously produces messages.

        Simulates a producer generating events.
        """
        for i in range(messages_per_publisher):
            await client.publish(test_subject, f"pub{publisher_id}_msg{i}".encode())
            await asyncio.sleep(0.01)  # Small delay to simulate realistic publishing

    # Start consumer task
    consumer = asyncio.create_task(consumer_task())

    # Start multiple publisher tasks
    publisher_tasks = [asyncio.create_task(publisher_task(i)) for i in range(num_publishers)]

    try:
        # Wait for all publishers to finish
        await asyncio.gather(*publisher_tasks)
        await client.flush()

        # Wait for consumer to receive all messages
        expected_count = messages_per_publisher * num_publishers
        max_wait = 5.0
        start = asyncio.get_event_loop().time()
        while len(received_messages) < expected_count:
            if asyncio.get_event_loop().time() - start > max_wait:
                break
            await asyncio.sleep(0.1)

        # Stop consumer task
        stop_iteration.set()
        await subscription.unsubscribe()
        await asyncio.wait_for(consumer, timeout=2.0)

        # Verify all messages received
        assert len(received_messages) == expected_count, (
            f"Expected {expected_count} messages, got {len(received_messages)}"
        )

        # Verify messages from all publishers
        for pub_id in range(num_publishers):
            pub_messages = [msg for msg in received_messages if msg.startswith(f"pub{pub_id}_")]
            assert len(pub_messages) == messages_per_publisher, (
                f"Publisher {pub_id} messages: expected {messages_per_publisher}, got {len(pub_messages)}"
            )

    finally:
        stop_iteration.set()
        for task in publisher_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if not consumer.done():
            consumer.cancel()
            try:
                await consumer
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_reconnect_preserves_subscription_during_publishing():
    """Test that subscriptions remain active after reconnection during active publishing.

    This ensures that the client properly re-establishes subscriptions on the
    new connection so that messages published after reconnection are received.
    """
    # Start initial server
    server = await run(port=0)
    server_port = server.port

    # Events
    disconnect_event = asyncio.Event()
    reconnect_event = asyncio.Event()

    # Connect client
    client = await connect(
        server.client_url,
        timeout=1.0,
        allow_reconnect=True,
        reconnect_time_wait=0.1,
    )

    client.add_disconnected_callback(disconnect_event.set)
    client.add_reconnected_callback(reconnect_event.set)

    # Create subscription
    test_subject = f"test.reconnect.subscription.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.flush()

    messages_received = []
    receive_task_running = True

    async def receive_messages():
        """Continuously receive messages."""
        while receive_task_running:
            try:
                msg = await subscription.next(timeout=0.1)
                messages_received.append(msg.data.decode())
            except TimeoutError:
                continue
            except Exception:
                # Expected during disconnect
                await asyncio.sleep(0.05)

    # Start receiver
    receive_task = asyncio.create_task(receive_messages())

    # Publishing control
    publish_task_running = True

    async def publish_messages():
        """Publish messages continuously."""
        counter = 0
        while publish_task_running:
            await client.publish(test_subject, f"message_{counter}".encode())
            counter += 1
            await asyncio.sleep(0.05)

    publish_task = asyncio.create_task(publish_messages())

    try:
        # Let some messages flow
        await asyncio.sleep(0.3)
        messages_before_disconnect = len(messages_received)
        assert messages_before_disconnect > 0, "Should receive messages before disconnect"

        # Trigger disconnect
        await server.shutdown()
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)

        # Restart server
        new_server = await run(port=server_port)

        try:
            # Wait for reconnection
            await asyncio.wait_for(reconnect_event.wait(), timeout=5.0)

            # Wait for messages to flow again
            await asyncio.sleep(0.5)

            messages_after_reconnect = len(messages_received)

            # Verify subscription is still active and receiving messages
            assert messages_after_reconnect > messages_before_disconnect, (
                f"Should receive messages after reconnect: "
                f"before={messages_before_disconnect}, after={messages_after_reconnect}"
            )

        finally:
            await new_server.shutdown()

    finally:
        publish_task_running = False
        receive_task_running = False
        await publish_task
        await receive_task
        await client.close()
