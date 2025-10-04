import asyncio
import uuid

import pytest
from nats.client import ClientStatus, NoRespondersError, connect
from nats.client.message import Headers
from nats.server import run


@pytest.mark.asyncio
async def test_connect_succeeds_with_valid_url(server):
    """Test that connecting to a valid server URL succeeds."""
    client = await connect(server.client_url, timeout=1.0)
    assert client.status == ClientStatus.CONNECTED
    assert client.server_info is not None
    await client.close()


@pytest.mark.asyncio
async def test_connect_fails_with_invalid_url():
    """Test that connecting to an invalid server URL fails appropriately."""
    with pytest.raises(Exception):
        await connect("nats://localhost:9999", timeout=0.5)


@pytest.mark.asyncio
async def test_publish_delivers_message_to_subscriber(client):
    """Test that a published message is delivered to a subscriber."""
    test_subject = f"test.{uuid.uuid4()}"
    test_message = b"Hello, NATS!"

    subscription = await client.subscribe(test_subject)
    await client.flush()  # Ensure subscription is registered

    await client.publish(test_subject, test_message)
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.data == test_message


@pytest.mark.asyncio
async def test_publish_sets_correct_subject(client):
    """Test that a published message has the correct subject."""
    test_subject = f"test.{uuid.uuid4()}"
    test_message = b"Hello, NATS!"

    subscription = await client.subscribe(test_subject)
    await client.flush()

    await client.publish(test_subject, test_message)
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.subject == test_subject


@pytest.mark.asyncio
async def test_publish_with_headers(client):
    """Test that a message can be published with headers."""
    test_subject = f"test.headers.{uuid.uuid4()}"
    test_headers = Headers({"key1": "value1", "key2": ["value2", "value3"]})
    test_payload = b"Message with headers"

    subscription = await client.subscribe(test_subject)
    await client.flush()

    await client.publish(test_subject, test_payload, headers=test_headers)
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.headers is not None
    assert message.headers.get("key1") == "value1"
    assert message.headers.get_all("key2") == ["value2", "value3"]


@pytest.mark.asyncio
async def test_request_reply_with_single_responder(client):
    """Test request-reply messaging pattern with a single responder."""
    test_subject = f"test.request.{uuid.uuid4()}"
    request_payload = b"Request data"
    reply_payload = b"Reply data"

    # Setup responder
    async def handle_request():
        subscription = await client.subscribe(test_subject)
        await client.flush()
        message = await subscription.next(timeout=1.0)
        await client.publish(message.reply_to, reply_payload)

    responder_task = asyncio.create_task(handle_request())
    await client.flush()

    # Send request
    response = await client.request(test_subject, request_payload, timeout=1.0)

    # Verify response
    assert response.data == reply_payload
    await responder_task


@pytest.mark.asyncio
async def test_flush_ensures_message_delivery(client):
    """Test that flush ensures all pending messages are delivered."""
    test_subject = f"test.flush.{uuid.uuid4()}"
    message_count = 10

    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish messages without awaiting between them
    for i in range(message_count):
        await client.publish(test_subject, f"{i}".encode())

    # Flush to ensure delivery
    await client.flush()

    # Verify all messages are received
    received_count = 0
    for _ in range(message_count):
        try:
            await subscription.next(timeout=0.5)
            received_count += 1
        except TimeoutError:
            break

    assert received_count == message_count


@pytest.mark.asyncio
async def test_client_as_context_manager(server):
    """Test that Client can be used as an async context manager."""
    async with await connect(server.client_url, timeout=1.0) as client:
        assert client.status == ClientStatus.CONNECTED

        # Verify we can publish and subscribe
        test_subject = f"test.context.{uuid.uuid4()}"
        async with await client.subscribe(test_subject) as subscription:
            await client.flush()
            await client.publish(test_subject, b"Context test")
            await client.flush()
            message = await subscription.next(timeout=1.0)
            assert message.data == b"Context test"

    # Client should be closed after exiting context
    assert client.status == ClientStatus.CLOSED


@pytest.mark.asyncio
async def test_client_close_stops_publishing(client):
    """Test that closing the client prevents further publishing."""
    test_subject = f"test.close.{uuid.uuid4()}"

    # Close the client
    await client.close()

    # Verify we can't publish anymore
    with pytest.raises(Exception):
        await client.publish(test_subject, b"Message after close")


@pytest.mark.asyncio
async def test_client_close_stops_subscribing(client):
    """Test that closing the client prevents further subscriptions."""
    test_subject = f"test.close.{uuid.uuid4()}"

    # Close the client
    await client.close()

    # Verify we can't subscribe anymore
    with pytest.raises(Exception):
        await client.subscribe(test_subject)


@pytest.mark.asyncio
async def test_client_close_updates_status(client):
    """Test that closing the client updates its status to CLOSED."""
    await client.close()
    assert client.status == ClientStatus.CLOSED


@pytest.mark.asyncio
async def test_disconnection_and_reconnection_callbacks(server):
    """Test that disconnection and reconnection callbacks are properly invoked.

    This test simulates a server disconnection and reconnection scenario:
    1. Create a client with disconnect/reconnect callbacks
    2. Stop the server to trigger disconnection
    3. Start a new server on the same port to trigger reconnection
    4. Verify both callbacks were invoked and client functionality is restored
    """
    # Events to track callback invocations
    disconnect_event = asyncio.Event()
    reconnect_event = asyncio.Event()

    # Connect client with callbacks and reconnection options
    client = await connect(
        server.client_url,
        timeout=1.0,
        allow_reconnect=True,
        reconnect_time_wait=0.1
    )

    # Register callbacks
    def on_disconnect():
        disconnect_event.set()

    def on_reconnect():
        reconnect_event.set()

    client.add_disconnected_callback(on_disconnect)
    client.add_reconnected_callback(on_reconnect)

    # Verify client is working before disconnect
    test_subject = f"test.reconnect.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.publish(test_subject, b"before disconnect")
    await client.flush()
    msg = await subscription.next(timeout=1.0)
    assert msg.data == b"before disconnect"

    # Save the server port to reuse it after shutdown
    server_port = server.port

    # Stop the server to trigger disconnect
    await server.shutdown()

    # Wait for disconnect callback
    try:
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set(), "Disconnect callback was not invoked"
    except asyncio.TimeoutError:
        pytest.fail("Disconnect callback was not invoked within timeout")

    # Start a new server on the same port
    new_server = await run(port=server_port)
    try:
        # Wait for reconnect callback
        try:
            await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
            assert reconnect_event.is_set(
            ), "Reconnect callback was not invoked"
        except asyncio.TimeoutError:
            pytest.fail("Reconnect callback was not invoked within timeout")

        # Verify client works after reconnection
        await client.publish(test_subject, b"after reconnect")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"after reconnect"
    finally:
        # Clean up resources
        await new_server.shutdown()
        await client.close()


@pytest.mark.asyncio
async def test_connect_with_ipv6_localhost(server):
    """Test connecting to server using IPv6 localhost address."""
    # Get the server port and construct IPv6 URL
    port = server.port
    ipv6_url = f"nats://[::1]:{port}"

    try:
        client = await connect(ipv6_url, timeout=1.0)
        assert client.status == ClientStatus.CONNECTED

        # Verify we can publish/subscribe
        test_subject = f"test.ipv6.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"IPv6 test")
        await client.flush()

        message = await subscription.next(timeout=1.0)
        assert message.data == b"IPv6 test"

        await client.close()
    except Exception as e:
        # IPv6 might not be available on all systems
        pytest.skip(f"IPv6 not available: {e}")


@pytest.mark.asyncio
async def test_reconnect_with_ipv6_address():
    """Test that reconnection works with IPv6 addresses in server pool."""
    # Start server on IPv6 localhost (let it pick a port)
    server = await run(host="::1", port=0)
    port = server.port

    # Connect using IPv6 URL
    ipv6_url = f"nats://[::1]:{port}"
    client = await connect(
        ipv6_url,
        timeout=1.0,
        allow_reconnect=True,
        reconnect_time_wait=0.1
    )

    # Verify connection works
    test_subject = f"test.ipv6.reconnect.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.publish(test_subject, b"before")
    await client.flush()
    msg = await subscription.next(timeout=1.0)
    assert msg.data == b"before"

    # Track reconnection
    reconnect_event = asyncio.Event()
    client.add_reconnected_callback(lambda: reconnect_event.set())

    # Shutdown and restart server
    await server.shutdown()
    new_server = await run(host="::1", port=port)

    # Wait for reconnection
    await asyncio.wait_for(reconnect_event.wait(), timeout=3.0)

    # Verify client works after reconnection
    await client.publish(test_subject, b"after")
    await client.flush()
    msg = await subscription.next(timeout=1.0)
    assert msg.data == b"after"

    await client.close()
    await new_server.shutdown()


@pytest.mark.asyncio
async def test_request_with_no_responders_raises_error(client):
    """Test that sending a request to a subject with no responders raises NoRespondersError."""
    test_subject = f"test.no_responders.{uuid.uuid4()}"
    request_payload = b"Request with no responders"

    # Send request to a subject with no subscribers/responders
    # The NATS server should automatically respond with "503 No Responders"
    # because no_responders=True is set in the CONNECT message
    with pytest.raises(NoRespondersError) as exc_info:
        await client.request(test_subject, request_payload, timeout=1.0)

    # Verify the exception details
    error = exc_info.value
    assert error.subject == test_subject
    assert error.status == "503"


@pytest.mark.asyncio
async def test_message_status_properties(client):
    """Test that Message status properties work correctly."""
    test_subject = f"test.status_properties.{uuid.uuid4()}"

    # Test no responders case (status 503)
    with pytest.raises(NoRespondersError):
        await client.request(test_subject, b"test", timeout=1.0)

    # Test with return_on_error=True to get the Message object
    response = await client.request(
        test_subject, b"test", timeout=1.0, return_on_error=True
    )

    # Verify status properties
    assert response.status.code == "503"
    assert response.has_status is True
    assert response.is_error_status is True

    # Test normal message (no status)
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish a normal message without status
    await client.publish(test_subject, b"normal message")
    await client.flush()

    normal_msg = await subscription.next(timeout=1.0)
    assert normal_msg.status is None
    assert normal_msg.has_status is False
    assert normal_msg.is_error_status is False


@pytest.mark.asyncio
async def test_multiple_disconnect_reconnect_callbacks(server):
    """Test that multiple disconnect and reconnect callbacks are all properly invoked.

    This test verifies that:
    1. Multiple disconnection callbacks are all invoked when a server disconnects
    2. Multiple reconnection callbacks are all invoked when a server reconnects
    3. Client functionality is restored after reconnection
    """
    # Counters and events to track callback invocations
    disconnect_count = 0
    reconnect_count = 0
    disconnect_event = asyncio.Event()
    reconnect_event = asyncio.Event()

    # Connect client with callbacks and reconnection options
    client = await connect(
        server.client_url,
        timeout=1.0,
        allow_reconnect=True,
        reconnect_time_wait=0.1
    )

    # Register multiple callbacks
    def on_disconnect1():
        nonlocal disconnect_count
        disconnect_count += 1
        if disconnect_count == 2:
            disconnect_event.set()

    def on_disconnect2():
        nonlocal disconnect_count
        disconnect_count += 1
        if disconnect_count == 2:
            disconnect_event.set()

    def on_reconnect1():
        nonlocal reconnect_count
        reconnect_count += 1
        if reconnect_count == 2:
            reconnect_event.set()

    def on_reconnect2():
        nonlocal reconnect_count
        reconnect_count += 1
        if reconnect_count == 2:
            reconnect_event.set()

    # Register all callbacks
    client.add_disconnected_callback(on_disconnect1)
    client.add_disconnected_callback(on_disconnect2)
    client.add_reconnected_callback(on_reconnect1)
    client.add_reconnected_callback(on_reconnect2)

    # Verify client is working before disconnect
    test_subject = f"test.multiple_callbacks.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.publish(test_subject, b"test message")
    await client.flush()
    msg = await subscription.next(timeout=1.0)
    assert msg.data == b"test message"

    # Save the server port to reuse it
    server_port = server.port

    # Stop the server to trigger disconnect
    await server.shutdown()

    # Wait for disconnect callbacks
    try:
        await asyncio.wait_for(disconnect_event.wait(), timeout=5.0)
        assert disconnect_count == 2, f"Expected 2 disconnect callbacks, got {disconnect_count}"
    except asyncio.TimeoutError:
        pytest.fail("Not all disconnect callbacks were invoked within timeout")

    # Start a new server on the same port
    new_server = await run(port=server_port)
    try:
        # Wait for reconnect callbacks
        try:
            await asyncio.wait_for(reconnect_event.wait(), timeout=5.0)
            assert reconnect_count == 2, f"Expected 2 reconnect callbacks, got {reconnect_count}"
        except asyncio.TimeoutError:
            pytest.fail(
                "Not all reconnect callbacks were invoked within timeout"
            )

        # Verify client works after reconnection
        await client.publish(test_subject, b"after reconnect")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"after reconnect"
    finally:
        # Clean up resources
        await new_server.shutdown()
        await client.close()
