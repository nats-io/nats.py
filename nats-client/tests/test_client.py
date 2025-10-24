import asyncio
import uuid

import pytest
from nats.client import ClientStatistics, ClientStatus, NoRespondersError, connect
from nats.client.message import Headers
from nats.server import run, run_cluster


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
async def test_connect_to_token_server_with_correct_token():
    """Test that client can connect to an auth token server with the correct token."""
    import os

    # Start server with token authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_token.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with correct token should succeed
        client = await connect(server.client_url, timeout=1.0, token="test_token_123")
        assert client.status == ClientStatus.CONNECTED
        assert client.server_info is not None

        # Verify we can publish and receive messages with valid auth
        test_subject = f"test.auth.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"test")
        await client.flush()

        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"test"

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_token_server_with_incorrect_token():
    """Test that connect raises an error when using an incorrect token."""
    import os

    # Start server with token authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_token.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with incorrect token should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, token="wrong_token", allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_token_server_with_missing_token():
    """Test that connect raises an error when connecting without a token to a secured server."""
    import os

    # Start server with token authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_token.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect without token should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_reconnect_with_token():
    """Test that client can reconnect to an auth token server after disconnection."""
    import os

    # Start server with token authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_token.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Events to track callback invocations
        disconnect_event = asyncio.Event()
        reconnect_event = asyncio.Event()

        # Connect client with auth token and reconnection enabled
        client = await connect(
            server.client_url,
            timeout=1.0,
            token="test_token_123",
            allow_reconnect=True,
            reconnect_time_wait=0.1,
        )

        # Register callbacks
        def on_disconnect():
            disconnect_event.set()

        def on_reconnect():
            reconnect_event.set()

        client.add_disconnected_callback(on_disconnect)
        client.add_reconnected_callback(on_reconnect)

        # Verify client is working before disconnect
        test_subject = f"test.reconnect.auth.{uuid.uuid4()}"
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
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set()

        # Start a new server on the same port with same auth config
        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)
        try:
            # Wait for reconnect callback
            await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
            assert reconnect_event.is_set()

            # Verify client works after reconnection with auth token preserved
            await client.publish(test_subject, b"after reconnect")
            await client.flush()
            msg = await subscription.next(timeout=1.0)
            assert msg.data == b"after reconnect"
        finally:
            await new_server.shutdown()
            await client.close()
    finally:
        # Ensure original server is shutdown if still running
        try:
            await server.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_reconnect_with_user_password():
    """Test that client can reconnect to a user/pass server after disconnection."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Events to track callback invocations
        disconnect_event = asyncio.Event()
        reconnect_event = asyncio.Event()

        # Connect client with user/password and reconnection enabled
        client = await connect(
            server.client_url,
            timeout=1.0,
            user="testuser",
            password="testpass",
            allow_reconnect=True,
            reconnect_time_wait=0.1,
        )

        # Register callbacks
        def on_disconnect():
            disconnect_event.set()

        def on_reconnect():
            reconnect_event.set()

        client.add_disconnected_callback(on_disconnect)
        client.add_reconnected_callback(on_reconnect)

        # Verify client is working before disconnect
        test_subject = f"test.reconnect.userpass.{uuid.uuid4()}"
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
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set()

        # Start a new server on the same port with same auth config
        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)
        try:
            # Wait for reconnect callback
            await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
            assert reconnect_event.is_set()

            # Verify client works after reconnection with credentials preserved
            await client.publish(test_subject, b"after reconnect")
            await client.flush()
            msg = await subscription.next(timeout=1.0)
            assert msg.data == b"after reconnect"
        finally:
            await new_server.shutdown()
            await client.close()
    finally:
        # Ensure original server is shutdown if still running
        try:
            await server.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_connect_to_nkey_server_with_correct_nkey():
    """Test that client can connect to an NKey server with the correct NKey."""
    import os

    # Start server with NKey authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_nkey.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with correct NKey should succeed
        # Seed corresponds to public key UBABIZX6SZFAKHK2KGUFD6QH53FDAH5QVCH2R5MJLFPEVYAW22QWQQCX
        nkey_seed = "SUAEIV5COV7ADQZE52WTYHVJQRV7WKJE5J7IBBJGATJTUUT2LVFGVXDPRQ"
        client = await connect(server.client_url, timeout=1.0, nkey_seed=nkey_seed)
        assert client.status == ClientStatus.CONNECTED
        assert client.server_info is not None

        # Verify we can publish and receive messages with valid auth
        test_subject = f"test.nkey.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"test")
        await client.flush()

        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"test"

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_nkey_server_with_incorrect_nkey():
    """Test that connect raises an error when using an incorrect NKey."""
    import os

    import nkeys
    from nacl.signing import SigningKey

    # Start server with NKey authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_nkey.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Generate a different NKey (not authorized on server)
        signing_key = SigningKey.generate().encode()
        src = nkeys.encode_seed(signing_key, prefix=nkeys.PREFIX_BYTE_USER)
        wrong_seed = nkeys.from_seed(src).seed.decode()

        # Connect with incorrect NKey should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, nkey_seed=wrong_seed, allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_nkey_server_with_missing_nkey():
    """Test that connect raises an error when connecting without an NKey to a secured server."""
    import os

    # Start server with NKey authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_nkey.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect without NKey should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_reconnect_with_nkey():
    """Test that client can reconnect to an NKey server after disconnection."""
    import os

    # Start server with NKey authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_nkey.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Events to track callback invocations
        disconnect_event = asyncio.Event()
        reconnect_event = asyncio.Event()

        # Connect client with NKey and reconnection enabled
        nkey_seed = "SUAEIV5COV7ADQZE52WTYHVJQRV7WKJE5J7IBBJGATJTUUT2LVFGVXDPRQ"
        client = await connect(
            server.client_url,
            timeout=1.0,
            nkey_seed=nkey_seed,
            allow_reconnect=True,
            reconnect_time_wait=0.1,
        )

        # Register callbacks
        def on_disconnect():
            disconnect_event.set()

        def on_reconnect():
            reconnect_event.set()

        client.add_disconnected_callback(on_disconnect)
        client.add_reconnected_callback(on_reconnect)

        # Verify client is working before disconnect
        test_subject = f"test.reconnect.nkey.{uuid.uuid4()}"
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
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set()

        # Start a new server on the same port with same auth config
        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)
        try:
            # Wait for reconnect callback
            await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
            assert reconnect_event.is_set()

            # Verify client works after reconnection with NKey preserved
            await client.publish(test_subject, b"after reconnect")
            await client.flush()
            msg = await subscription.next(timeout=1.0)
            assert msg.data == b"after reconnect"
        finally:
            await new_server.shutdown()
            await client.close()
    finally:
        # Ensure original server is shutdown if still running
        try:
            await server.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_correct_credentials():
    """Test that client can connect to a user/pass server with correct credentials."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with correct credentials should succeed
        client = await connect(server.client_url, timeout=1.0, user="testuser", password="testpass")
        assert client.status == ClientStatus.CONNECTED
        assert client.server_info is not None

        # Verify we can publish and receive messages with valid auth
        test_subject = f"test.auth.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"test")
        await client.flush()

        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"test"

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_incorrect_password():
    """Test that connect raises an error when using an incorrect password."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with incorrect password should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, user="testuser", password="wrongpass", allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_missing_credentials():
    """Test that connect raises an error when connecting without credentials to a secured server."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect without credentials should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_user_only():
    """Test that server rejects connection when only username is provided without password."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with only username should raise ConnectionError (server rejects incomplete credentials)
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, user="testuser", allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_password_only():
    """Test that server rejects connection when only password is provided without username."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with only password should raise ConnectionError (server rejects incomplete credentials)
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, password="testpass", allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


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
    client = await connect(server.client_url, timeout=1.0, allow_reconnect=True, reconnect_time_wait=0.1)

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
            assert reconnect_event.is_set(), "Reconnect callback was not invoked"
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
    client = await connect(ipv6_url, timeout=1.0, allow_reconnect=True, reconnect_time_wait=0.1)

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
    assert isinstance(error, NoRespondersError)
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
    response = await client.request(test_subject, b"test", timeout=1.0, return_on_error=True)

    # Verify status properties
    assert response.status is not None
    assert response.status.code == "503"

    # Test normal message (no status)
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish a normal message without status
    await client.publish(test_subject, b"normal message")
    await client.flush()

    normal_msg = await subscription.next(timeout=1.0)
    assert normal_msg.status is None


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
    client = await connect(server.client_url, timeout=1.0, allow_reconnect=True, reconnect_time_wait=0.1)

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
            pytest.fail("Not all reconnect callbacks were invoked within timeout")

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
@pytest.mark.parametrize("cluster_size", [3, 5])
async def test_cluster_reconnect_sequential_shutdown(cluster_size):
    """Test client reconnection when cluster servers are shut down sequentially.

    This test verifies that:
    1. Client connects to a cluster with multiple servers
    2. Client reconnects as servers are shut down one by one in sequence
    3. Client maintains functionality throughout the sequential shutdowns
    4. Client continues to work as long as at least one server is available
    """
    # Start a cluster with the specified size
    cluster = await run_cluster(size=cluster_size)

    try:
        # Track reconnection events
        reconnect_count = 0
        reconnect_event = asyncio.Event()

        def on_reconnect():
            nonlocal reconnect_count
            reconnect_count += 1
            reconnect_event.set()

        # Connect to the first server - cluster will gossip other servers via INFO
        client = await connect(
            cluster.servers[0].client_url,
            timeout=0.5,
            allow_reconnect=True,
            reconnect_max_attempts=60,
            reconnect_time_wait=0.0,
            no_randomize=True,
        )

        client.add_reconnected_callback(on_reconnect)

        # Verify client is working
        test_subject = f"test.cluster.sequential.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"initial message")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"initial message"

        # Shut down servers one by one (shut down the server we're connected to)
        for i in range(len(cluster.servers) - 1):  # Keep last server running
            # Find which server the client is currently connected to using server_info
            assert client.server_info is not None
            connected_port = client.server_info.port

            # Find the matching server in the cluster by port
            server_to_shutdown = None
            for server in cluster.servers:
                if server.port == connected_port:
                    server_to_shutdown = server
                    break

            assert server_to_shutdown is not None, f"Could not find server for port {connected_port}"

            # Shutdown the connected server
            await server_to_shutdown.shutdown()

            # Wait for reconnection to another server
            reconnect_event.clear()
            try:
                await asyncio.wait_for(reconnect_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pytest.fail(f"Client did not reconnect after shutting down server {i}")

            # Give the client time to fully re-establish subscriptions
            await asyncio.sleep(0.2)
            await client.flush()

            # Verify client still works after reconnection
            await client.publish(test_subject, f"message after shutdown {i}".encode())
            await client.flush()
            msg = await subscription.next(timeout=5.0)
            assert msg.data == f"message after shutdown {i}".encode()

        # Verify we had the expected number of reconnections (cluster_size - 1)
        expected_reconnects = cluster_size - 1
        assert reconnect_count == expected_reconnects, (
            f"Expected {expected_reconnects} reconnects, got {reconnect_count}"
        )

        await client.close()

    finally:
        await cluster.shutdown()


@pytest.mark.asyncio
async def test_new_inbox(server):
    """Test that new_inbox generates unique inbox subjects with the configured prefix."""
    custom_prefix = "_MY_INBOX"
    client = await connect(server.client_url, inbox_prefix=custom_prefix, timeout=1.0)

    try:
        # Generate multiple inboxes
        inbox1 = client.new_inbox()
        inbox2 = client.new_inbox()
        inbox3 = client.new_inbox()

        # All should start with the custom prefix
        assert inbox1.startswith(custom_prefix)
        assert inbox2.startswith(custom_prefix)
        assert inbox3.startswith(custom_prefix)

        # All should be unique
        assert inbox1 != inbox2
        assert inbox1 != inbox3
        assert inbox2 != inbox3

    finally:
        await client.close()


@pytest.mark.asyncio
async def test_custom_inbox_prefix(server):
    """Test that custom inbox prefix is used for request-reply inboxes."""
    custom_prefix = "_MY_CUSTOM_INBOX"

    # Connect with custom inbox prefix
    client = await connect(server.client_url, inbox_prefix=custom_prefix, timeout=1.0)

    try:
        test_subject = f"test.custom_inbox.{uuid.uuid4()}"
        request_payload = b"Request data"
        reply_payload = b"Reply data"

        # Track the inbox subject used in the request
        received_reply_to = None

        # Setup responder that captures the reply-to subject
        subscription = await client.subscribe(test_subject)
        await client.flush()

        async def handle_request():
            nonlocal received_reply_to
            message = await subscription.next(timeout=2.0)
            received_reply_to = message.reply_to
            assert received_reply_to is not None
            await client.publish(received_reply_to, reply_payload)

        responder_task = asyncio.create_task(handle_request())

        # Send request
        response = await client.request(test_subject, request_payload, timeout=2.0)

        # Verify response
        assert response.data == reply_payload
        await responder_task

        # Verify that the inbox used the custom prefix
        assert received_reply_to is not None
        assert received_reply_to.startswith(custom_prefix), (
            f"Expected inbox to start with '{custom_prefix}', got '{received_reply_to}'"
        )

    finally:
        await client.close()


@pytest.mark.asyncio
async def test_max_outstanding_pings_closes_connection():
    """Test that connection closes when max outstanding pings is exceeded."""

    async def mock_server(reader, writer):
        """Mock NATS server that stops responding to PINGs."""
        # Send INFO
        info = b'INFO {"server_id":"test","version":"2.0.0","go":"go1.20","host":"127.0.0.1","port":4222,"headers":true,"max_payload":1048576}\r\n'
        writer.write(info)
        await writer.drain()

        # Read CONNECT from client
        await reader.readline()

        # Read and respond to first PING
        await reader.readline()
        writer.write(b"PONG\r\n")
        await writer.drain()

        # Now stop responding to PINGs - just read them without PONGing
        # This will cause outstanding pings to accumulate
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    # Start mock server
    server = await asyncio.start_server(mock_server, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()
    server_url = f"nats://{addr[0]}:{addr[1]}"

    try:
        # Connect with short ping interval and low max pings
        client = await connect(
            server_url,
            ping_interval=0.05,  # Ping every 50ms
            max_outstanding_pings=2,
            allow_reconnect=False,
            timeout=1.0,
        )

        try:
            # Verify client starts connected
            assert client.status == ClientStatus.CONNECTED

            # Wait for outstanding pings to accumulate and trigger disconnect
            # With ping_interval=0.05 and max=2, should disconnect after ~150ms
            await asyncio.sleep(0.3)

            # Verify client is no longer connected (closed due to max pings exceeded)
            assert client.status == ClientStatus.CLOSED, f"Expected CLOSED status, got {client.status}"
        finally:
            await client.close()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_inbox_prefix_cannot_be_empty(server):
    """Test that empty inbox prefix is rejected."""
    with pytest.raises(ValueError, match="inbox_prefix cannot be empty"):
        await connect(server.client_url, inbox_prefix="", timeout=1.0)


@pytest.mark.asyncio
async def test_inbox_prefix_cannot_contain_greater_than_wildcard(server):
    """Test that inbox prefix with '>' wildcard is rejected."""
    with pytest.raises(ValueError, match="inbox_prefix cannot contain '>' wildcard"):
        await connect(server.client_url, inbox_prefix="test.>", timeout=1.0)


@pytest.mark.asyncio
async def test_inbox_prefix_cannot_contain_asterisk_wildcard(server):
    """Test that inbox prefix with '*' wildcard is rejected."""
    with pytest.raises(ValueError, match=r"inbox_prefix cannot contain '\*' wildcard"):
        await connect(server.client_url, inbox_prefix="test.*", timeout=1.0)


@pytest.mark.asyncio
async def test_inbox_prefix_cannot_end_with_dot(server):
    """Test that inbox prefix ending with '.' is rejected."""
    with pytest.raises(ValueError, match="inbox_prefix cannot end with '.'"):
        await connect(server.client_url, inbox_prefix="test.", timeout=1.0)


@pytest.mark.asyncio
async def test_server_initiated_ping_pong():
    """Test that client properly handles PING from server and responds with PONG."""
    import os

    # Start server with very short ping interval
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_ping.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        client = await connect(server.client_url, timeout=1.0, allow_reconnect=False)

        try:
            # Wait long enough for server to send at least one PING
            # Server is configured to ping every 100ms
            await asyncio.sleep(0.3)

            # If ping/pong handling didn't work, client would be disconnected
            assert client.status == ClientStatus.CONNECTED, "Client should still be connected after server PINGs"
        finally:
            await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_reconnect_while_publishing():
    """Test that client can reconnect while actively publishing messages.

    This test verifies that:
    1. Client continues to publish messages during normal operation
    2. When the server disconnects, the client detects the disconnection
    3. Publishing blocks during reconnection (waiting for connection)
    4. Client successfully reconnects to a new server
    5. Publishing resumes after reconnection
    6. Messages published after reconnection are successfully delivered
    """
    # Start initial server
    server = await run(port=0)
    server_port = server.port

    # Events to track lifecycle
    disconnect_event = asyncio.Event()
    reconnect_event = asyncio.Event()

    # Connect client with reconnection enabled
    client = await connect(
        server.client_url,
        timeout=1.0,
        allow_reconnect=True,
        reconnect_time_wait=0.1,
        reconnect_max_attempts=100,
    )

    client.add_disconnected_callback(disconnect_event.set)
    client.add_reconnected_callback(reconnect_event.set)

    # Set up subscription to verify messages
    test_subject = f"test.reconnect.load.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Counters for tracking
    messages_sent_before_disconnect = 0
    messages_sent_after_reconnect = 0
    publish_task_running = True

    async def publish_continuously():
        """Continuously publish messages until told to stop.

        publish() will block during reconnection, not raise exceptions.
        """
        nonlocal messages_sent_before_disconnect, messages_sent_after_reconnect
        counter = 0

        while publish_task_running:
            message = f"message_{counter}".encode()
            # This may block during reconnection but won't raise
            await client.publish(test_subject, message)
            counter += 1

            # Track message counts based on connection state
            if reconnect_event.is_set():
                messages_sent_after_reconnect += 1
            elif not disconnect_event.is_set():
                messages_sent_before_disconnect += 1

            # Small delay to simulate realistic publish rate
            await asyncio.sleep(0.01)

    # Start publishing task
    publish_task = asyncio.create_task(publish_continuously())

    try:
        # Let some messages publish successfully
        await asyncio.sleep(0.2)
        assert messages_sent_before_disconnect > 0, "Should have published messages before disconnect"

        # Verify we're receiving messages
        msg = await subscription.next(timeout=1.0)
        assert msg.data.startswith(b"message_")

        # Shutdown server while publishing is active
        await server.shutdown()

        # Wait for disconnect to be detected
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set()

        # Start new server on same port
        new_server = await run(port=server_port)

        try:
            # Wait for reconnection
            await asyncio.wait_for(reconnect_event.wait(), timeout=5.0)
            assert reconnect_event.is_set()

            # Give time for publishing to resume
            await asyncio.sleep(0.3)

            # Verify publishing resumed after reconnection
            assert messages_sent_after_reconnect > 0, "Should have published messages after reconnect"

            # Verify we can receive messages after reconnection
            await client.flush()
            msg = await subscription.next(timeout=2.0)
            assert msg.data.startswith(b"message_")

        finally:
            await new_server.shutdown()

    finally:
        # Stop publishing task
        publish_task_running = False
        await publish_task
        await client.close()


@pytest.mark.asyncio
async def test_reconnect_with_high_volume_publishing():
    """Test reconnection behavior under high message volume.

    This test verifies that the client can handle reconnection even when
    publishing a large number of messages rapidly, ensuring buffering and
    flow control work correctly across reconnection boundaries.
    """
    # Start initial server
    server = await run(port=0)
    server_port = server.port

    # Events to track lifecycle
    disconnect_event = asyncio.Event()
    reconnect_event = asyncio.Event()

    # Connect client
    client = await connect(
        server.client_url,
        timeout=1.0,
        allow_reconnect=True,
        reconnect_time_wait=0.1,
        reconnect_max_attempts=100,
    )

    client.add_disconnected_callback(disconnect_event.set)
    client.add_reconnected_callback(reconnect_event.set)

    test_subject = f"test.reconnect.highvolume.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Track successful publishes
    successful_publishes = 0
    publish_task_running = True

    async def publish_high_volume():
        """Publish messages rapidly - will block during reconnection."""
        nonlocal successful_publishes
        counter = 0

        while publish_task_running:
            # Publish rapidly - may block during reconnection
            await client.publish(test_subject, f"msg_{counter}".encode())
            successful_publishes += 1
            counter += 1
            # Small sleep every N messages to prevent overwhelming
            if counter % 50 == 0:
                await asyncio.sleep(0.01)

    # Start high-volume publishing
    publish_task = asyncio.create_task(publish_high_volume())

    try:
        # Let messages accumulate
        await asyncio.sleep(0.2)
        publishes_before = successful_publishes
        assert publishes_before > 50, f"Should have published many messages, got {publishes_before}"

        # Trigger disconnect during heavy load
        await server.shutdown()
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)

        # Restart server
        new_server = await run(port=server_port)

        try:
            # Wait for reconnection
            await asyncio.wait_for(reconnect_event.wait(), timeout=5.0)

            # Let publishing resume
            await asyncio.sleep(0.2)
            publishes_after = successful_publishes

            # Verify publishing continued after reconnection
            assert publishes_after > publishes_before, (
                f"Publishing should resume after reconnect: before={publishes_before}, after={publishes_after}"
            )

            # Verify we can still receive messages
            await client.flush()
            msg = await subscription.next(timeout=2.0)
            assert msg.data.startswith(b"msg_")

        finally:
            await new_server.shutdown()

    finally:
        publish_task_running = False
        await publish_task
        await client.close()


@pytest.mark.asyncio
async def test_reconnect_with_multiple_concurrent_publishers():
    """Test reconnection with multiple publishing tasks running concurrently.

    This simulates a realistic scenario where multiple application components
    are publishing to different subjects simultaneously when a reconnection occurs.
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
        reconnect_max_attempts=100,
    )

    client.add_disconnected_callback(disconnect_event.set)
    client.add_reconnected_callback(reconnect_event.set)

    # Create multiple subjects and subscriptions
    num_subjects = 5
    subjects = [f"test.subject.{i}.{uuid.uuid4()}" for i in range(num_subjects)]
    subscriptions = []

    for subject in subjects:
        sub = await client.subscribe(subject)
        subscriptions.append(sub)
    await client.flush()

    # Track publishes per subject
    publish_counts = {subject: 0 for subject in subjects}
    publish_lock = asyncio.Lock()
    tasks_running = True

    async def publish_to_subject(subject: str):
        """Publish continuously to a specific subject."""
        counter = 0
        while tasks_running:
            await client.publish(subject, f"{subject}_msg_{counter}".encode())
            async with publish_lock:
                publish_counts[subject] += 1
            counter += 1
            await asyncio.sleep(0.02)

    # Start multiple publishing tasks
    publish_tasks = [asyncio.create_task(publish_to_subject(subject)) for subject in subjects]

    try:
        # Let all publishers run
        await asyncio.sleep(0.3)

        # Verify all subjects are being published to
        async with publish_lock:
            for subject, count in publish_counts.items():
                assert count > 0, f"Subject {subject} should have messages"

        # Trigger disconnect
        await server.shutdown()
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)

        # Restart server
        new_server = await run(port=server_port)

        try:
            # Wait for reconnection
            await asyncio.wait_for(reconnect_event.wait(), timeout=5.0)

            # Let publishing resume
            await asyncio.sleep(0.3)

            # Verify all subjects resume publishing
            async with publish_lock:
                counts_before = dict(publish_counts)

            await asyncio.sleep(0.2)

            async with publish_lock:
                counts_after = dict(publish_counts)

            for subject in subjects:
                assert counts_after[subject] > counts_before[subject], (
                    f"Subject {subject} should continue publishing after reconnect"
                )

            # Verify we can receive on all subscriptions
            await client.flush()
            for i, subscription in enumerate(subscriptions):
                msg = await subscription.next(timeout=2.0)
                assert subjects[i].encode() in msg.data

        finally:
            await new_server.shutdown()

    finally:
        tasks_running = False
        await asyncio.gather(*publish_tasks)
        await client.close()


@pytest.mark.asyncio
async def test_client_drain_closes_connection(client):
    """Test that drain closes the connection."""
    # Verify client is connected
    assert client.status == ClientStatus.CONNECTED

    # Drain the client
    await client.drain()

    # Verify client is closed
    assert client.status == ClientStatus.CLOSED


@pytest.mark.asyncio
async def test_client_drain_processes_pending_messages(server):
    """Test that drain allows pending messages in subscriptions to be processed."""
    client = await connect(server.client_url, timeout=1.0)

    try:
        test_subject = f"test.drain.pending.{uuid.uuid4()}"

        # Create subscription
        subscription = await client.subscribe(test_subject)
        await client.flush()

        # Publish multiple messages
        message_count = 10
        for i in range(message_count):
            await client.publish(test_subject, f"message-{i}".encode())
        await client.flush()

        # Wait for messages to arrive
        await asyncio.sleep(0.1)

        # Drain the client (should allow pending messages to be processed)
        drain_task = asyncio.create_task(client.drain())

        # Read all pending messages before drain completes
        messages_received = []
        try:
            while len(messages_received) < message_count:
                msg = await asyncio.wait_for(subscription.next(), timeout=1.0)
                messages_received.append(msg.data.decode())
        except (RuntimeError, asyncio.TimeoutError):
            # Expected when subscription is drained
            pass

        # Wait for drain to complete
        await drain_task

        # Verify we received all messages
        assert len(messages_received) == message_count
        for i in range(message_count):
            assert f"message-{i}" in messages_received

    finally:
        if client.status != ClientStatus.CLOSED:
            await client.close()


@pytest.mark.asyncio
async def test_client_drain_flushes_pending_publishes(server):
    """Test that drain flushes pending published messages."""
    # Create two clients: one publisher, one subscriber
    publisher = await connect(server.client_url, timeout=1.0)
    subscriber = await connect(server.client_url, timeout=1.0)

    try:
        test_subject = f"test.drain.flush.{uuid.uuid4()}"

        # Create subscription on subscriber client
        subscription = await subscriber.subscribe(test_subject)
        await subscriber.flush()

        # Publish messages without flushing
        message_count = 5
        for i in range(message_count):
            await publisher.publish(test_subject, f"message-{i}".encode())

        # Drain should flush these pending messages
        await publisher.drain()

        # Verify subscriber receives all messages
        messages_received = []
        for _ in range(message_count):
            try:
                msg = await asyncio.wait_for(subscription.next(), timeout=2.0)
                messages_received.append(msg.data.decode())
            except asyncio.TimeoutError:
                break

        assert len(messages_received) == message_count, (
            f"Expected {message_count} messages, got {len(messages_received)}"
        )
        for i in range(message_count):
            assert f"message-{i}" in messages_received

    finally:
        if publisher.status != ClientStatus.CLOSED:
            await publisher.close()
        if subscriber.status != ClientStatus.CLOSED:
            await subscriber.close()


@pytest.mark.asyncio
async def test_client_drain_multiple_subscriptions(server):
    """Test that drain handles multiple subscriptions correctly."""
    client = await connect(server.client_url, timeout=1.0)

    try:
        # Create multiple subscriptions
        num_subscriptions = 5
        subjects = [f"test.drain.multi.{uuid.uuid4()}.{i}" for i in range(num_subscriptions)]
        subscriptions = []

        for subject in subjects:
            sub = await client.subscribe(subject)
            subscriptions.append(sub)
        await client.flush()

        # Publish messages to each subscription
        messages_per_sub = 3
        for subject in subjects:
            for i in range(messages_per_sub):
                await client.publish(subject, f"{subject}-msg-{i}".encode())
        await client.flush()

        # Wait for messages to arrive
        await asyncio.sleep(0.1)

        # Drain the client
        drain_task = asyncio.create_task(client.drain())

        # Collect messages from all subscriptions
        all_messages = []

        async def collect_messages(sub):
            messages = []
            try:
                while True:
                    msg = await asyncio.wait_for(sub.next(), timeout=1.0)
                    messages.append(msg.data.decode())
            except (RuntimeError, asyncio.TimeoutError):
                pass
            return messages

        # Collect from all subscriptions concurrently
        collection_tasks = [asyncio.create_task(collect_messages(sub)) for sub in subscriptions]
        results = await asyncio.gather(*collection_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, list):
                all_messages.extend(result)

        # Wait for drain to complete
        await drain_task

        # Verify we received all messages
        expected_count = num_subscriptions * messages_per_sub
        assert len(all_messages) == expected_count

    finally:
        if client.status != ClientStatus.CLOSED:
            await client.close()


@pytest.mark.asyncio
async def test_client_drain_with_custom_timeout(server):
    """Test that drain accepts a custom timeout parameter."""
    client = await connect(server.client_url, timeout=1.0)

    try:
        test_subject = f"test.drain.timeout.{uuid.uuid4()}"

        # Create subscription
        await client.subscribe(test_subject)
        await client.flush()

        # Publish a few messages
        for i in range(5):
            await client.publish(test_subject, f"message-{i}".encode())
        await client.flush()

        # Drain with a generous timeout - should complete successfully
        await client.drain(timeout=5.0)

        # Client should be closed
        assert client.status == ClientStatus.CLOSED

    finally:
        if client.status != ClientStatus.CLOSED:
            await client.close()


@pytest.mark.asyncio
async def test_client_drain_on_already_closed_client(server):
    """Test that drain is idempotent when called on already closed client."""
    client = await connect(server.client_url, timeout=1.0)

    # Close the client
    await client.close()
    assert client.status == ClientStatus.CLOSED

    # Try to drain - should return without error (idempotent behavior, matching Go)
    await client.drain()

    # Should still be closed
    assert client.status == ClientStatus.CLOSED


@pytest.mark.asyncio
async def test_client_drain_multiple_calls_idempotent(server):
    """Test that calling drain multiple times is idempotent (following Go semantics)."""
    client = await connect(server.client_url, timeout=1.0)

    try:
        test_subject = f"test.drain.multiple.{uuid.uuid4()}"

        # Create subscription
        await client.subscribe(test_subject)
        await client.flush()

        # Call drain multiple times - all should succeed without error
        await client.drain()
        await client.drain()  # Second call - should be no-op
        await client.drain()  # Third call - should be no-op

        # Verify client is closed
        assert client.status == ClientStatus.CLOSED

    finally:
        if client.status != ClientStatus.CLOSED:
            await client.close()


@pytest.mark.asyncio
async def test_client_drain_disables_reconnect(server):
    """Test that drain disables automatic reconnection."""
    client = await connect(
        server.client_url,
        timeout=1.0,
        allow_reconnect=True,
        reconnect_max_attempts=10,
    )

    try:
        # Verify reconnect is enabled
        assert client._allow_reconnect is True

        # Start draining
        await client.drain()

        # Verify reconnect has been disabled
        assert client._allow_reconnect is False
        assert client.status == ClientStatus.CLOSED

    finally:
        if client.status != ClientStatus.CLOSED:
            await client.close()


@pytest.mark.asyncio
async def test_client_drain_with_no_subscriptions(server):
    """Test that drain works correctly even with no active subscriptions."""
    client = await connect(server.client_url, timeout=1.0)

    try:
        # Drain without any subscriptions
        await client.drain()

        # Client should be closed
        assert client.status == ClientStatus.CLOSED

    finally:
        if client.status != ClientStatus.CLOSED:
            await client.close()


@pytest.mark.asyncio
async def test_client_drain_preferred_over_close(server):
    """Test that drain is the preferred way to shutdown (following Go semantics)."""
    # This test demonstrates the recommended usage pattern
    client = await connect(server.client_url, timeout=1.0)

    test_subject = f"test.drain.preferred.{uuid.uuid4()}"

    # Create subscription and publish messages
    subscription = await client.subscribe(test_subject)
    await client.flush()

    for i in range(5):
        await client.publish(test_subject, f"message-{i}".encode())
    await client.flush()

    # Wait for messages to arrive
    await asyncio.sleep(0.1)

    # Use drain instead of close - this is the preferred pattern
    drain_task = asyncio.create_task(client.drain())

    # Can still process pending messages during drain
    messages = []
    try:
        while True:
            msg = await asyncio.wait_for(subscription.next(), timeout=0.5)
            messages.append(msg.data.decode())
    except (RuntimeError, asyncio.TimeoutError):
        pass

    await drain_task

    # Verify we processed messages before shutdown
    assert len(messages) > 0
    assert client.status == ClientStatus.CLOSED

    # Note: No need to call close() after drain() - drain handles it


@pytest.mark.asyncio
async def test_statistics_initial_values(client):
    """Test that statistics start at zero."""
    stats = client.stats()

    assert isinstance(stats, ClientStatistics)
    assert stats.in_msgs == 0
    assert stats.out_msgs == 0
    assert stats.in_bytes == 0
    assert stats.out_bytes == 0
    assert stats.reconnects == 0


@pytest.mark.asyncio
async def test_statistics_publish_counts(client):
    """Test that publishing messages increments out_msgs and out_bytes."""
    await client.publish("test.subject", b"Hello")
    await client.publish("test.subject", b"World!")
    await client.flush()

    stats = client.stats()
    assert stats.out_msgs == 2
    assert stats.out_bytes == len(b"Hello") + len(b"World!")


@pytest.mark.asyncio
async def test_statistics_subscribe_counts(client):
    """Test that receiving messages increments in_msgs and in_bytes."""
    sub = await client.subscribe("test.stats")

    await client.publish("test.stats", b"Test message")
    await client.flush()

    msg = await sub.next(timeout=1.0)
    assert msg.data == b"Test message"

    stats = client.stats()
    assert stats.in_msgs == 1
    assert stats.in_bytes == len(b"Test message")
    assert stats.out_msgs == 1


@pytest.mark.asyncio
async def test_statistics_multiple_messages(client):
    """Test statistics with multiple messages."""
    sub = await client.subscribe("test.multiple")

    messages = [b"Message 1", b"Message 2", b"Message 3"]
    for msg_data in messages:
        await client.publish("test.multiple", msg_data)
    await client.flush()

    received = []
    for _ in range(len(messages)):
        msg = await sub.next(timeout=1.0)
        received.append(msg.data)

    assert received == messages

    stats = client.stats()
    assert stats.in_msgs == 3
    assert stats.out_msgs == 3

    total_bytes = sum(len(m) for m in messages)
    assert stats.in_bytes == total_bytes
    assert stats.out_bytes == total_bytes


@pytest.mark.asyncio
async def test_statistics_with_headers(client):
    """Test that statistics count payload bytes, not protocol overhead."""
    sub = await client.subscribe("test.headers")

    payload = b"Test payload"
    headers = {"X-Custom": "value"}

    await client.publish("test.headers", payload, headers=headers)
    await client.flush()

    msg = await sub.next(timeout=1.0)
    assert msg.data == payload

    stats = client.stats()
    assert stats.out_bytes == len(payload)
    assert stats.in_bytes == len(payload)


@pytest.mark.asyncio
async def test_statistics_request_reply(client):
    """Test statistics with request/reply pattern."""
    sub = await client.subscribe("test.request")

    async def handle_request():
        msg = await sub.next(timeout=2.0)
        await client.publish(msg.reply_to, b"Response")

    request_task = asyncio.create_task(handle_request())
    await asyncio.sleep(0.1)

    response = await client.request("test.request", b"Request", timeout=1.0)
    assert response.data == b"Response"

    await request_task

    stats = client.stats()
    assert stats.out_msgs == 2
    assert stats.in_msgs == 2
    assert stats.out_bytes == len(b"Request") + len(b"Response")


@pytest.mark.asyncio
async def test_statistics_snapshot(client):
    """Test that stats() returns a snapshot, not a reference."""
    stats1 = client.stats()

    await client.publish("test.snapshot", b"Data")
    await client.flush()

    stats2 = client.stats()

    assert stats1.out_msgs == 0
    assert stats1.out_bytes == 0
    assert stats2.out_msgs == 1
    assert stats2.out_bytes == len(b"Data")


@pytest.mark.asyncio
async def test_statistics_reconnect_counter(server):
    """Test that reconnects are counted."""
    async with await connect(server.client_url, reconnect_time_wait=0.1) as client:
        initial_stats = client.stats()
        assert initial_stats.reconnects == 0

        await client._connection.close()
        await asyncio.sleep(0.5)

        stats = client.stats()
        assert stats.reconnects >= 1


@pytest.mark.asyncio
async def test_subscription_pending_messages_limit(client):
    """Test that messages are dropped when pending_msgs_limit is exceeded."""
    from nats.client import SlowConsumerError

    test_subject = f"test.slow_consumer.msgs.{uuid.uuid4()}"

    # Track slow consumer errors
    slow_consumer_errors = []

    def on_error(error):
        if isinstance(error, SlowConsumerError):
            slow_consumer_errors.append(error)

    client.add_error_callback(on_error)

    # Create subscription with low message limit
    subscription = await client.subscribe(test_subject, max_pending_messages=5)
    await client.flush()

    # Publish more messages than the limit without consuming
    num_messages = 20
    for i in range(num_messages):
        await client.publish(test_subject, f"message-{i}".encode())
    await client.flush()

    # Wait for messages to arrive and trigger slow consumer
    await asyncio.sleep(0.2)

    # Verify slow consumer error was triggered
    assert len(slow_consumer_errors) == 1, "Should have received exactly one slow consumer error"
    error = slow_consumer_errors[0]
    assert error.subject == test_subject
    assert error.pending_messages >= 5

    # Verify pending count
    pending_msgs, pending_bytes = subscription.pending
    assert pending_msgs <= 5, f"Should not exceed limit of 5, got {pending_msgs}"

    # Consume available messages (should be approximately the limit)
    consumed = 0
    while True:
        try:
            await asyncio.wait_for(subscription.next(), timeout=0.1)
            consumed += 1
        except asyncio.TimeoutError:
            break

    # Should have consumed around the limit, not all messages
    assert consumed <= 6, f"Should have consumed around the limit, got {consumed}"
    assert consumed < num_messages, "Should not have received all messages (some dropped)"


@pytest.mark.asyncio
async def test_subscription_pending_bytes_limit(client):
    """Test that messages are dropped when pending_bytes_limit is exceeded."""
    from nats.client import SlowConsumerError

    test_subject = f"test.slow_consumer.bytes.{uuid.uuid4()}"

    # Track slow consumer errors
    slow_consumer_errors = []

    def on_error(error):
        if isinstance(error, SlowConsumerError):
            slow_consumer_errors.append(error)

    client.add_error_callback(on_error)

    # Create subscription with low byte limit (100 bytes)
    subscription = await client.subscribe(test_subject, max_pending_bytes=100)
    await client.flush()

    # Publish messages that will exceed the byte limit
    # Each message is 50 bytes, so 3 messages = 150 bytes > 100 byte limit
    large_message = b"x" * 50
    num_messages = 10
    for i in range(num_messages):
        await client.publish(test_subject, large_message)
    await client.flush()

    # Wait for messages to arrive and trigger slow consumer
    await asyncio.sleep(0.2)

    # Verify slow consumer error was triggered
    assert len(slow_consumer_errors) == 1, "Should have received exactly one slow consumer error"
    error = slow_consumer_errors[0]
    assert error.subject == test_subject
    assert error.pending_bytes <= 150, "Pending bytes should be near limit"

    # Verify pending count
    pending_msgs, pending_bytes = subscription.pending
    assert pending_bytes <= 150, f"Should not far exceed limit, got {pending_bytes}"

    # Consume available messages
    consumed = 0
    while True:
        try:
            await asyncio.wait_for(subscription.next(), timeout=0.1)
            consumed += 1
        except asyncio.TimeoutError:
            break

    # Should have consumed only a few messages, not all
    assert consumed < num_messages, "Should not have received all messages (some dropped)"


@pytest.mark.asyncio
async def test_slow_consumer_error_only_once(client):
    """Test that slow consumer error is only reported once per slow event."""
    from nats.client import SlowConsumerError

    test_subject = f"test.slow_consumer.once.{uuid.uuid4()}"

    # Track slow consumer errors
    slow_consumer_errors = []

    def on_error(error):
        if isinstance(error, SlowConsumerError):
            slow_consumer_errors.append(error)

    client.add_error_callback(on_error)

    # Create subscription with low limit
    await client.subscribe(test_subject, max_pending_messages=5)
    await client.flush()

    # Publish many messages to trigger slow consumer multiple times
    for i in range(50):
        await client.publish(test_subject, f"message-{i}".encode())
        await client.flush()
        await asyncio.sleep(0.01)  # Small delay to ensure messages are processed

    # Wait for processing
    await asyncio.sleep(0.2)

    # Should only get ONE slow consumer error, not multiple
    assert len(slow_consumer_errors) == 1, (
        f"Should have received exactly one slow consumer error, got {len(slow_consumer_errors)}"
    )


@pytest.mark.asyncio
async def test_slow_consumer_flag_resets_when_under_limit(client):
    """Test that slow consumer flag resets when pending count drops below limit."""
    from nats.client import SlowConsumerError

    test_subject = f"test.slow_consumer.reset.{uuid.uuid4()}"

    # Track slow consumer errors
    slow_consumer_errors = []

    def on_error(error):
        if isinstance(error, SlowConsumerError):
            slow_consumer_errors.append(error)

    client.add_error_callback(on_error)

    # Create subscription with low limit
    subscription = await client.subscribe(test_subject, max_pending_messages=3)
    await client.flush()

    # Publish messages to trigger slow consumer
    for i in range(10):
        await client.publish(test_subject, f"message-{i}".encode())
    await client.flush()
    await asyncio.sleep(0.1)

    # Should have triggered slow consumer
    assert len(slow_consumer_errors) == 1

    # Consume messages to get below limit
    for _ in range(3):
        try:
            await asyncio.wait_for(subscription.next(), timeout=0.5)
        except asyncio.TimeoutError:
            break

    # Wait a bit
    await asyncio.sleep(0.1)

    # Publish more messages to trigger slow consumer again
    for i in range(10):
        await client.publish(test_subject, f"message2-{i}".encode())
    await client.flush()
    await asyncio.sleep(0.1)

    # Should have triggered slow consumer a SECOND time (flag was reset)
    assert len(slow_consumer_errors) == 2, (
        f"Expected 2 slow consumer errors after reset, got {len(slow_consumer_errors)}"
    )


@pytest.mark.asyncio
async def test_unlimited_pending_with_none_limit(client):
    """Test that None limit means unlimited pending messages."""
    test_subject = f"test.unlimited.{uuid.uuid4()}"

    # Create subscription with unlimited limits (None)
    subscription = await client.subscribe(test_subject, max_pending_messages=None, max_pending_bytes=None)
    await client.flush()

    # Publish many messages
    num_messages = 100
    for i in range(num_messages):
        await client.publish(test_subject, f"message-{i}".encode())
    await client.flush()

    # Wait for all messages to arrive
    await asyncio.sleep(0.3)

    # Consume all messages
    consumed = 0
    while consumed < num_messages:
        try:
            await asyncio.wait_for(subscription.next(), timeout=1.0)
            consumed += 1
        except asyncio.TimeoutError:
            break

    # Should have received ALL messages (no limit)
    assert consumed == num_messages, f"Expected {num_messages} messages, got {consumed}"


@pytest.mark.asyncio
async def test_subscription_pending_method(client):
    """Test that pending() method returns correct counts."""
    test_subject = f"test.pending_method.{uuid.uuid4()}"

    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Initial pending should be zero
    pending_msgs, pending_bytes = subscription.pending
    assert pending_msgs == 0
    assert pending_bytes == 0

    # Publish messages
    messages = [b"message1", b"message22", b"message333"]
    for msg in messages:
        await client.publish(test_subject, msg)
    await client.flush()

    # Wait for messages to arrive
    await asyncio.sleep(0.1)

    # Check pending
    pending_msgs, pending_bytes = subscription.pending
    assert pending_msgs == len(messages)
    expected_bytes = sum(len(m) for m in messages)
    assert pending_bytes == expected_bytes

    # Consume one message
    await subscription.next(timeout=1.0)

    # Check pending decreased
    pending_msgs, pending_bytes = subscription.pending
    assert pending_msgs == len(messages) - 1
    assert pending_bytes == expected_bytes - len(messages[0])

    # Consume remaining
    await subscription.next(timeout=1.0)
    await subscription.next(timeout=1.0)

    # Check pending is zero again
    pending_msgs, pending_bytes = subscription.pending
    assert pending_msgs == 0
    assert pending_bytes == 0


@pytest.mark.asyncio
async def test_slow_consumer_with_headers(client):
    """Test that slow consumer correctly counts bytes for messages with headers."""
    from nats.client import SlowConsumerError

    test_subject = f"test.slow_consumer.headers.{uuid.uuid4()}"

    slow_consumer_errors = []

    def on_error(error):
        if isinstance(error, SlowConsumerError):
            slow_consumer_errors.append(error)

    client.add_error_callback(on_error)

    # Create subscription with low byte limit
    # Note: byte limit counts ONLY payload, not headers
    subscription = await client.subscribe(test_subject, max_pending_bytes=100)
    await client.flush()

    # Publish messages with headers
    # Payload is 50 bytes, so 3 messages = 150 bytes > 100 byte limit
    payload = b"x" * 50
    headers = {"X-Test": "value"}

    for i in range(10):
        await client.publish(test_subject, payload, headers=headers)
    await client.flush()

    # Wait for processing
    await asyncio.sleep(0.2)

    # Should trigger slow consumer (counts payload bytes only)
    assert len(slow_consumer_errors) == 1

    # Consume available messages
    consumed = 0
    while True:
        try:
            msg = await asyncio.wait_for(subscription.next(), timeout=0.1)
            # Verify message has headers
            assert msg.headers is not None
            consumed += 1
        except asyncio.TimeoutError:
            break

    # Should have dropped some messages
    assert consumed < 10


@pytest.mark.asyncio
async def test_subscription_default_limits(client):
    """Test that default pending limits are applied."""
    test_subject = f"test.default_limits.{uuid.uuid4()}"

    # Create subscription with default limits
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Verify internal limits are set to defaults
    # Default: 65536 messages, 64 MB
    assert subscription._max_pending_messages == 65536
    assert subscription._max_pending_bytes == 67108864  # 64 * 1024 * 1024


@pytest.mark.asyncio
async def test_subscription_dropped_counters(client):
    """Test that dropped message counters are updated when messages are dropped."""
    test_subject = f"test.dropped.{uuid.uuid4()}"

    # Create subscription with very low limits
    subscription = await client.subscribe(test_subject, max_pending_messages=2, max_pending_bytes=100)
    await client.flush()

    # Verify dropped counters start at zero
    dropped_msgs, dropped_bytes = subscription.dropped
    assert dropped_msgs == 0
    assert dropped_bytes == 0

    # Publish enough messages to exceed limits
    for i in range(10):
        await client.publish(test_subject, b"test message")
    await client.flush()

    # Wait for messages to arrive
    await asyncio.sleep(0.1)

    # Verify some messages were dropped
    dropped_msgs, dropped_bytes = subscription.dropped
    assert dropped_msgs > 0, "Should have dropped some messages"
    assert dropped_bytes > 0, "Should have dropped some bytes"

    # Verify pending is at or near limit
    pending_msgs, pending_bytes = subscription.pending
    assert pending_msgs <= 2, "Pending should not exceed limit"

    # Verify dropped count increases as we publish more
    initial_dropped = dropped_msgs
    for i in range(5):
        await client.publish(test_subject, b"more messages")
    await client.flush()
    await asyncio.sleep(0.1)

    dropped_msgs, dropped_bytes = subscription.dropped
    assert dropped_msgs > initial_dropped, "Dropped count should increase"
