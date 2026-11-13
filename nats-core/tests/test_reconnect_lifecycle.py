"""Reconnect work belongs to the client, independently of the triggering task."""

import asyncio
import contextlib

import nats.client as client_module
import pytest
from nats.client import ClientStatus, connect


async def finish_task(task):
    if task is not None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_cancelling_force_reconnect_caller_keeps_recovery_alive(server, monkeypatch):
    client = await connect(server.client_url, reconnect_time_wait=0)
    entered = asyncio.Event()
    release = asyncio.Event()
    reconnected = asyncio.Event()
    client.add_reconnected_callback(reconnected.set)
    establish = client_module.establish_connection

    async def paused_establish(*args, **kwargs):
        entered.set()
        await release.wait()
        return await establish(*args, **kwargs)

    monkeypatch.setattr(client_module, "establish_connection", paused_establish)
    caller = asyncio.create_task(client.force_reconnect())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert client.status == ClientStatus.RECONNECTING
        await client.force_reconnect()  # Wake the existing cycle; do not create another.
        release.set()
        await asyncio.wait_for(reconnected.wait(), 2)
        assert client.status == ClientStatus.CONNECTED
        sub = await client.subscribe("recovered")
        await client.publish("recovered", b"ok")
        await client.flush(timeout=1)
        assert (await sub.next(timeout=1)).data == b"ok"
    finally:
        release.set()
        await client.close()
        await finish_task(caller)


@pytest.mark.parametrize("trigger", ["force", "read", "flush"])
async def test_close_stops_reconnect_during_backoff(server, monkeypatch, trigger):
    client = await connect(server.client_url, reconnect_time_wait=30, reconnect_jitter=0)
    disconnected = asyncio.Event()
    client.add_disconnected_callback(disconnected.set)
    caller = None
    try:
        if trigger == "force":
            caller = asyncio.create_task(client.force_reconnect())
        elif trigger == "read":
            await server.shutdown()
        else:

            async def ignore_pong():
                pass

            monkeypatch.setattr(client, "_handle_pong", ignore_pong)
            caller = asyncio.create_task(client.flush(timeout=0.01))
        await asyncio.wait_for(disconnected.wait(), 2)
        await asyncio.wait_for(client.close(), 1)
        assert client.status == ClientStatus.CLOSED
        if caller is not None:
            assert caller.done(), "close() left a reconnect caller running"
        assert client._reconnect_task is None
    finally:
        await client.close()
        await finish_task(caller)


@pytest.mark.parametrize("phase", ["info", "pong"])
@pytest.mark.parametrize("reconnecting", [False, True], ids=["initial", "reconnect"])
async def test_cancelled_handshake_closes_candidate(phase, reconnecting):
    connections = []
    handlers = set()
    parked = asyncio.Event()
    release = asyncio.Event()

    async def serve(reader, writer):
        handlers.add(asyncio.current_task())
        connections.append((reader, writer))
        attempt = len(connections)
        try:
            if (attempt > 1 or not reconnecting) and phase == "info":
                parked.set()
                await release.wait()
            writer.write(
                b'INFO {"server_id":"fake","version":"1","go":"go1","host":"127.0.0.1","port":4222,"headers":true}\r\n'
            )
            await writer.drain()
            while line := await reader.readline():
                if line == b"PING\r\n":
                    if (attempt > 1 or not reconnecting) and phase == "pong":
                        parked.set()
                        await release.wait()
                    writer.write(b"PONG\r\n")
                    await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            handlers.discard(asyncio.current_task())

    listener = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    url = f"nats://127.0.0.1:{port}"
    client = await connect(url, reconnect_time_wait=0, reconnect_timeout=30) if reconnecting else None
    caller = asyncio.create_task(client.force_reconnect() if client is not None else connect(url, timeout=30))
    try:
        await asyncio.wait_for(parked.wait(), 2)
        if client is not None:
            await asyncio.wait_for(client.close(), 1)
            assert caller.done()
            assert client.status == ClientStatus.CLOSED
        else:
            caller.cancel()
            with pytest.raises(asyncio.CancelledError):
                await caller
        assert await asyncio.wait_for(connections[-1][0].read(), 1) == b""
    finally:
        release.set()
        if client is not None:
            await client.close()
        await finish_task(caller)
        listener.close()
        await listener.wait_closed()
        for _, writer in connections:
            writer.close()
        await asyncio.gather(*handlers, return_exceptions=True)


@pytest.mark.parametrize("blocked_write", ["subscription", "pending"])
async def test_close_during_restore_does_not_commit_connection(server, monkeypatch, blocked_write):
    client = await connect(server.client_url, reconnect_time_wait=0)
    await client.subscribe("restore")
    await client.flush(timeout=1)
    original_connection = client._connection
    establish = client_module.establish_connection
    entered = asyncio.Event()
    connect_allowed = asyncio.Event()
    restoring = asyncio.Event()
    release_write = asyncio.Event()
    candidates = []

    async def paused_establish(*args, **kwargs):
        entered.set()
        await connect_allowed.wait()
        connection, info, tls = await establish(*args, **kwargs)
        candidates.append(connection)
        write = connection.write

        async def paused_write(data):
            prefix = b"SUB " if blocked_write == "subscription" else b"PUB "
            if data.startswith(prefix):
                restoring.set()
                await release_write.wait()
            await write(data)

        monkeypatch.setattr(connection, "write", paused_write)
        return connection, info, tls

    monkeypatch.setattr(client_module, "establish_connection", paused_establish)
    caller = asyncio.create_task(client.force_reconnect())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await client.publish("restore", b"queued while disconnected")
        connect_allowed.set()
        await asyncio.wait_for(restoring.wait(), 2)
        assert client.status == ClientStatus.RECONNECTING
        assert client._connection is original_connection
        await asyncio.wait_for(client.close(), 1)
        assert not candidates[0].is_connected()
        assert client.status == ClientStatus.CLOSED
        assert client._reconnect_task is None
    finally:
        connect_allowed.set()
        release_write.set()
        await client.close()
        await finish_task(caller)


async def test_exhaustion_releases_subscription_and_request_waiters(server, monkeypatch):
    client = await connect(server.client_url, reconnect_time_wait=0, reconnect_max_attempts=1)
    subscription = await client.subscribe("empty")
    responder = await client.subscribe("unanswered")
    await client.flush(timeout=1)
    next_message = asyncio.create_task(subscription.next())
    request = asyncio.create_task(client.request("unanswered", b"request", timeout=30))
    await responder.next(timeout=1)  # The request is now registered and awaiting its reply.

    async def unavailable(*args, **kwargs):
        raise ConnectionError("unavailable")

    monkeypatch.setattr(client_module, "establish_connection", unavailable)
    try:
        await asyncio.wait_for(client.force_reconnect(), 1)
        assert client.status == ClientStatus.CLOSED
        assert client._reconnect_task is None
        with pytest.raises(RuntimeError, match="Subscription is closed"):
            await asyncio.wait_for(next_message, 1)
        with pytest.raises(asyncio.CancelledError):
            await request
        assert not client._subscriptions
        assert not client._request_futures
    finally:
        await client.close()
        await finish_task(next_message)
        await finish_task(request)


async def test_wake_during_failed_handshake_skips_next_backoff(server, monkeypatch):
    client = await connect(server.client_url, reconnect_time_wait=30, reconnect_jitter=0)
    disconnected = asyncio.Event()
    entered = asyncio.Event()
    release = asyncio.Event()
    reconnected = asyncio.Event()
    client.add_disconnected_callback(disconnected.set)
    client.add_reconnected_callback(reconnected.set)
    establish = client_module.establish_connection
    attempts = 0

    async def fail_first(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            entered.set()
            await release.wait()
            raise ConnectionError("first attempt failed")
        return await establish(*args, **kwargs)

    monkeypatch.setattr(client_module, "establish_connection", fail_first)
    caller = asyncio.create_task(client.force_reconnect())
    try:
        await asyncio.wait_for(disconnected.wait(), 1)
        await client.force_reconnect()
        await asyncio.wait_for(entered.wait(), 1)
        await client.force_reconnect()
        release.set()
        await asyncio.wait_for(reconnected.wait(), 1)
        await caller
        assert attempts == 2
    finally:
        release.set()
        await client.close()
        await finish_task(caller)


async def test_ipv6_seed_and_reconnect_keep_same_pool_identity(server, monkeypatch):
    establish = client_module.establish_connection
    attempted = []

    async def connect_ipv6_via_test_server(url, **kwargs):
        attempted.append(url)
        if "127.0.0.1:1" in url:
            raise ConnectionError("unavailable peer")
        connection, info, tls = await establish(server.client_url, **kwargs)
        info["connect_urls"] = ["[::1]:4222", "127.0.0.1:1"]
        return connection, info, tls

    monkeypatch.setattr(client_module, "establish_connection", connect_ipv6_via_test_server)
    client = await connect("nats://[::1]:4222", reconnect_time_wait=0, no_randomize=True)
    try:
        assert [s.url for s in client._server_pool] == ["nats://[::1]:4222", "nats://127.0.0.1:1"]
        await client.force_reconnect()
        assert client._last_server is not None
        assert client._last_server.url == "nats://[::1]:4222"
        await client.force_reconnect()
        assert attempted == [
            "nats://[::1]:4222",
            "nats://127.0.0.1:1",
            "nats://[::1]:4222",
            "nats://127.0.0.1:1",
            "nats://[::1]:4222",
        ]
        assert [s.url for s in client._server_pool] == ["nats://[::1]:4222", "nats://127.0.0.1:1"]
    finally:
        await client.close()


@pytest.mark.parametrize("failure", ["write", "parse"])
async def test_failed_handshake_closes_candidate(server, monkeypatch, failure):
    client = await connect(server.client_url, reconnect_time_wait=0, reconnect_max_attempts=1)
    establish = client_module.establish_connection
    candidates = []

    async def broken_candidate(*args, **kwargs):
        connection, info, tls = await establish(*args, **kwargs)
        candidates.append(connection)

        async def failed_write(data):
            raise ConnectionResetError("failed CONNECT write")

        async def malformed_response():
            return b"NOT-A-PONG\r\n"

        if failure == "write":
            monkeypatch.setattr(connection, "write", failed_write)
        else:
            monkeypatch.setattr(connection, "readline", malformed_response)
        return connection, info, tls

    monkeypatch.setattr(client_module, "establish_connection", broken_candidate)
    errors = []
    client.add_error_callback(errors.append)
    try:
        await asyncio.wait_for(client.force_reconnect(), 1)
        assert client.status == ClientStatus.CLOSED
        assert not candidates[0].is_connected()
        assert len(errors) == 1
    finally:
        await client.close()


async def test_close_before_reconnect_task_starts(server):
    client = await connect(server.client_url)
    caller = asyncio.create_task(client.force_reconnect())
    try:
        await asyncio.sleep(0)
        await asyncio.wait_for(client.close(), 1)
        assert client.status == ClientStatus.CLOSED
        assert client._reconnect_task is None
        assert caller.done()
    finally:
        await client.close()
        await finish_task(caller)


@pytest.mark.parametrize("operation", ["close", "reconnect"])
async def test_stalled_old_connection_close_is_bounded(server, monkeypatch, operation):
    import nats.client.connection as connection_module

    client = await connect(server.client_url, reconnect_time_wait=0)
    old = client._connection
    close = old.close

    async def stalled_close():
        await close()
        await asyncio.Future()

    monkeypatch.setattr(old, "close", stalled_close)
    monkeypatch.setattr(connection_module, "ABANDONED_CLOSE_TIMEOUT", 0.02)
    try:
        if operation == "close":
            await asyncio.wait_for(client.close(), 1)
            assert client.status == ClientStatus.CLOSED
        else:
            await asyncio.wait_for(client.force_reconnect(), 1)
            assert client.status == ClientStatus.CONNECTED
            assert client._connection is not old
    finally:
        await client.close()
