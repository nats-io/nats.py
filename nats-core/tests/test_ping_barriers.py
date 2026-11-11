"""Each flush, RTT probe, and heartbeat owns its position in the PONG stream."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import pytest
from nats.client import connect
from nats.client.connection import TcpConnection


@dataclass
class Peer:
    writer: asyncio.StreamWriter
    pings: asyncio.Queue[bytes] = field(default_factory=asyncio.Queue)
    pongs: asyncio.Queue[bytes] = field(default_factory=asyncio.Queue)
    closed: asyncio.Event = field(default_factory=asyncio.Event)

    async def next_ping(self):
        assert await asyncio.wait_for(self.pings.get(), timeout=2) == b"PING\r\n"

    async def pong(self):
        # The server PING follows this PONG on the same TCP stream. Receiving
        # the client's reply confirms it processed the preceding PONG, without
        # relying on a sleep before inspecting the waiting calls.
        self.writer.write(b"PONG\r\nPING\r\n")
        await self.writer.drain()
        assert await asyncio.wait_for(self.pongs.get(), timeout=2) == b"PONG\r\n"


@asynccontextmanager
async def protocol_server():
    accepted = asyncio.Queue()
    peers = []
    tasks = []

    async def serve(reader, writer):
        peer = Peer(writer)
        peers.append(peer)
        accepted.put_nowait(peer)
        try:
            writer.write(
                b'INFO {"server_id":"test","version":"2.0.0","go":"go1.20",'
                b'"host":"127.0.0.1","port":4222,"headers":true}\r\n'
            )
            await writer.drain()
            handshake = True
            while line := await reader.readline():
                if line == b"PING\r\n" and handshake:
                    handshake = False
                    writer.write(b"PONG\r\n")
                    await writer.drain()
                elif line == b"PING\r\n":
                    peer.pings.put_nowait(line)
                elif line == b"PONG\r\n":
                    peer.pongs.put_nowait(line)
        finally:
            peer.closed.set()
            writer.close()

    def accept(reader, writer):
        tasks.append(asyncio.create_task(serve(reader, writer)))

    listener = await asyncio.start_server(accept, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    try:
        yield f"nats://127.0.0.1:{port}", accepted
    finally:
        listener.close()
        for peer in peers:
            peer.writer.transport.abort()
        for task in tasks:
            if not task.done():
                task.cancel()
        results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2)
        await asyncio.wait_for(listener.wait_closed(), timeout=2)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result


@asynccontextmanager
async def connected_client(**options):
    async with protocol_server() as (url, accepted):
        client = await connect(url, timeout=2, allow_reconnect=False, **options)
        pending = []
        try:
            peer = await asyncio.wait_for(accepted.get(), timeout=2)
            yield client, peer, pending
        finally:
            for task in pending:
                task.cancel()
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=2)
            await asyncio.wait_for(client.close(), timeout=2)


async def start_call(client, peer, pending, method):
    task = asyncio.create_task(getattr(client, method)())
    pending.append(task)
    await peer.next_ping()
    return task


@pytest.mark.parametrize(
    ("first_method", "second_method"),
    [("flush", "flush"), ("rtt", "rtt"), ("flush", "rtt"), ("rtt", "flush")],
)
async def test_one_pong_completes_only_its_matching_call(first_method, second_method):
    async with connected_client() as (client, peer, pending):
        first = await start_call(client, peer, pending, first_method)
        second = await start_call(client, peer, pending, second_method)

        await peer.pong()
        await asyncio.wait_for(asyncio.shield(first), timeout=2)
        assert not second.done(), "The first PONG completed the later call"

        await peer.pong()
        await asyncio.wait_for(asyncio.shield(second), timeout=2)


@pytest.mark.parametrize(
    ("method", "interruption"),
    [("flush", "cancel"), ("rtt", "cancel"), ("rtt", "timeout"), ("flush", "timeout")],
)
async def test_interrupted_call_keeps_its_pong_slot(method, interruption):
    async with connected_client() as (client, peer, pending):
        if interruption == "cancel":
            abandoned = await start_call(client, peer, pending, method)
            abandoned.cancel()
            with pytest.raises(asyncio.CancelledError):
                await abandoned
        else:
            if method == "rtt":
                call = client.rtt(timeout=0.05)
            else:
                # Preserve flush's existing native timeout policy: cancellation
                # by an external deadline is the behavior under test here.
                call = asyncio.wait_for(client.flush(), timeout=0.05)
            abandoned = asyncio.create_task(call)
            pending.append(abandoned)
            await peer.next_ping()
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(abandoned), timeout=2)

        current = await start_call(client, peer, pending, "flush")
        await peer.pong()
        assert not current.done(), "An abandoned call's PONG completed the next flush"

        await peer.pong()
        await asyncio.wait_for(asyncio.shield(current), timeout=2)


async def test_heartbeat_pong_cannot_complete_later_flush():
    async with connected_client(ping_interval=0.05) as (client, peer, pending):
        await peer.next_ping()
        # The first background heartbeat is now on the wire. Prevent subsequent
        # heartbeats from competing with the manually acknowledged flush.
        client._ping_interval = 3600
        current = await start_call(client, peer, pending, "flush")

        await peer.pong()
        assert not current.done(), "A heartbeat PONG completed a later flush"

        await peer.pong()
        await asyncio.wait_for(asyncio.shield(current), timeout=2)


@pytest.mark.parametrize("termination", ["close", "disconnect"])
async def test_connection_termination_fails_pending_flush_and_rtt(termination):
    async with connected_client() as (client, peer, pending):
        flush = await start_call(client, peer, pending, "flush")
        rtt = await start_call(client, peer, pending, "rtt")

        if termination == "close":
            await asyncio.wait_for(client.close(), timeout=2)
        else:
            peer.writer.close()
            await asyncio.wait_for(peer.writer.wait_closed(), timeout=2)

        for task in (flush, rtt):
            with pytest.raises(ConnectionError):
                await asyncio.wait_for(asyncio.shield(task), timeout=2)


async def test_sequential_flush_and_rtt_succeed_with_matching_pongs():
    async with connected_client() as (client, peer, pending):
        for method in ("flush", "rtt", "flush"):
            task = await start_call(client, peer, pending, method)
            assert not task.done()
            await peer.pong()
            result = await asyncio.wait_for(asyncio.shield(task), timeout=2)
            if method == "rtt":
                assert isinstance(result, float)
                assert result >= 0
            else:
                assert result is None


@pytest.mark.parametrize("stage", ["before-write", "after-write"])
@pytest.mark.parametrize("interruption", ["cancel", "error"])
async def test_interrupted_ping_write_closes_socket_and_fails_other_waiters(monkeypatch, stage, interruption):
    async with connected_client() as (client, peer, pending):
        waiting = await start_call(client, peer, pending, "flush")
        entered = asyncio.Event()
        release = asyncio.Event()
        original_write = TcpConnection.write
        error = OSError("Interrupted PING write")

        async def write(connection, data):
            if data != b"PING\r\n":
                return await original_write(connection, data)
            if stage == "after-write":
                await original_write(connection, data)
            entered.set()
            await release.wait()
            raise error

        monkeypatch.setattr(TcpConnection, "write", write)
        interrupted = asyncio.create_task(client.rtt())
        pending.append(interrupted)
        await asyncio.wait_for(entered.wait(), timeout=2)
        if stage == "after-write":
            await peer.next_ping()

        if interruption == "cancel":
            interrupted.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(interrupted), timeout=2)
        else:
            release.set()
            with pytest.raises(OSError) as raised:
                await asyncio.wait_for(asyncio.shield(interrupted), timeout=2)
            assert raised.value is error

        await asyncio.wait_for(peer.closed.wait(), timeout=2)
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(asyncio.shield(waiting), timeout=2)


async def test_repeated_rtt_timeouts_cannot_suppress_stale_connection_detection():
    async with connected_client(ping_interval=0.05, max_outstanding_pings=2) as (client, peer, pending):

        async def probe_repeatedly():
            while True:
                try:
                    await client.rtt(timeout=0.005)
                except TimeoutError:
                    continue
                except ConnectionError:
                    return

        probes = asyncio.create_task(probe_repeatedly())
        pending.append(probes)
        await peer.next_ping()
        # Continuous public probes must not reset the background heartbeat
        # deadline forever when the server never responds to any PING.
        await asyncio.wait_for(peer.closed.wait(), timeout=2)
        await asyncio.wait_for(asyncio.shield(probes), timeout=2)


async def test_cancelled_rtt_waiting_to_write_does_not_reserve_a_pong(monkeypatch):
    async with connected_client() as (client, peer, pending):
        release = asyncio.Event()
        original_write = TcpConnection.write
        first_ping = True

        async def write(connection, data):
            nonlocal first_ping
            await original_write(connection, data)
            if data == b"PING\r\n" and first_ping:
                first_ping = False
                await release.wait()

        monkeypatch.setattr(TcpConnection, "write", write)
        first = await start_call(client, peer, pending, "flush")
        started = asyncio.Event()

        async def measure_rtt():
            started.set()
            return await client.rtt()

        # The flush's write still holds the PING lock. The RTT task runs until
        # it blocks acquiring that lock, before it can reserve a PONG slot.
        cancelled = asyncio.create_task(measure_rtt())
        pending.append(cancelled)
        await asyncio.wait_for(started.wait(), timeout=2)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(cancelled), timeout=2)

        release.set()
        await peer.pong()
        await asyncio.wait_for(asyncio.shield(first), timeout=2)
        assert peer.pings.empty(), "The cancelled RTT wrote a PING while waiting for the lock"
        assert not peer.closed.is_set()

        current = await start_call(client, peer, pending, "flush")
        await peer.pong()
        await asyncio.wait_for(asyncio.shield(current), timeout=2)


async def test_reconnect_fails_old_flush_and_new_flush_requires_its_own_pong():
    async with protocol_server() as (url, accepted):
        client = await connect(url, timeout=2, reconnect_time_wait=0.01, reconnect_jitter=0, reconnect_max_attempts=1)
        pending = []
        reconnected = asyncio.Event()
        client.add_reconnected_callback(reconnected.set)
        try:
            first_peer = await asyncio.wait_for(accepted.get(), timeout=2)
            old = await start_call(client, first_peer, pending, "flush")
            first_peer.writer.close()
            await asyncio.wait_for(first_peer.writer.wait_closed(), timeout=2)

            with pytest.raises(ConnectionError):
                await asyncio.wait_for(asyncio.shield(old), timeout=2)
            new_peer = await asyncio.wait_for(accepted.get(), timeout=2)
            await asyncio.wait_for(reconnected.wait(), timeout=2)

            current = await start_call(client, new_peer, pending, "flush")
            assert not current.done(), "The reconnect handshake PONG completed a later flush"
            await new_peer.pong()
            await asyncio.wait_for(asyncio.shield(current), timeout=2)
        finally:
            for task in pending:
                task.cancel()
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=2)
            await asyncio.wait_for(client.close(), timeout=2)
