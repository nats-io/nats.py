"""Fetch deadlines must leave time to receive replies after server expiry."""

import asyncio
import contextlib
from datetime import timedelta

import pytest
import pytest_asyncio
from nats.client import connect
from nats.jetstream import new


@pytest_asyncio.fixture
async def delayed_client(server, request):
    """Delay downstream TCP bytes after setup, without changing broker replies."""
    delay = asyncio.Event()
    reply_delay = getattr(request, "param", 0.15)
    handlers = set()

    async def proxy(reader, writer):
        handler = asyncio.current_task()
        handlers.add(handler)
        upstream_writer = None
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", server.port)

            async def copy(source, destination, downstream=False):
                try:
                    while chunk := await source.read(65536):
                        if downstream and delay.is_set():
                            await asyncio.sleep(reply_delay)
                        destination.write(chunk)
                        await destination.drain()
                finally:
                    destination.close()

            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(copy(reader, upstream_writer))
                tasks.create_task(copy(upstream_reader, writer, downstream=True))
        finally:
            for peer in (writer, upstream_writer):
                if peer is not None:
                    peer.close()
                    with contextlib.suppress(OSError):
                        await peer.wait_closed()
            handlers.discard(handler)

    try:
        async with await asyncio.start_server(proxy, "127.0.0.1", 0) as listener:
            port = listener.sockets[0].getsockname()[1]
            client = await connect(f"nats://127.0.0.1:{port}")
            try:
                yield client, delay
            finally:
                await client.close()
    finally:
        pending = list(handlers)
        for handler in pending:
            handler.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.parametrize("operation", ["messages", "partial", "bytes", "next"])
async def test_fetch_keeps_delayed_first_delivery(jetstream, delayed_client, operation):
    """A delivery sent before expiry remains valid when its TCP reply is delayed."""
    stream = await jetstream.create_stream(name="DELAYED", subjects=["DELAYED"])
    await stream.create_consumer(name="c", ack_policy="explicit", max_expires=timedelta(seconds=0.05))
    await jetstream.publish("DELAYED", b"one")
    client, delay = delayed_client
    consumer = await new(client).get_consumer("DELAYED", "c")
    delay.set()

    async with asyncio.timeout(2):
        if operation == "next":
            received = [await consumer.next(max_wait=0.05)]
        else:
            if operation == "bytes":
                batch = await consumer.fetch(max_bytes=1024, max_wait=0.05)
            else:
                batch = await consumer.fetch(max_messages=2 if operation == "partial" else 1, max_wait=0.05)
            received = [message async for message in batch]
            assert batch.error is None

        assert [message.data for message in received] == [b"one"]
        assert received[0].metadata.num_delivered == 1
        await received[0].double_ack()

    info = await jetstream.get_consumer_info("DELAYED", "c")
    assert info.delivered["consumer_seq"] == 1
    assert info.num_ack_pending == 0


async def test_fetch_finishes_on_delayed_expiry_status(jetstream, delayed_client):
    """An empty fetch ends on the server's status without waiting out the margin."""
    stream = await jetstream.create_stream(name="EMPTY", subjects=["EMPTY"])
    await stream.create_consumer(name="c", max_expires=timedelta(seconds=0.05))
    client, delay = delayed_client
    consumer = await new(client).get_consumer("EMPTY", "c")
    delay.set()

    loop = asyncio.get_running_loop()
    started = loop.time()
    async with asyncio.timeout(0.8):
        batch = await consumer.fetch(max_messages=1, max_wait=0.05)
        assert [message async for message in batch] == []
    assert loop.time() - started >= 0.15
    assert batch.error is None


@pytest.mark.parametrize("delayed_client", [10.0], indirect=True)
async def test_fetch_bounds_wait_for_missing_reply(jetstream, delayed_client):
    """A missing terminal status still has a finite client receive deadline."""
    stream = await jetstream.create_stream(name="SILENT", subjects=["SILENT"])
    await stream.create_consumer(name="c")
    client, delay = delayed_client
    consumer = await new(client).get_consumer("SILENT", "c")
    delay.set()

    async with asyncio.timeout(2):
        batch = await consumer.fetch(max_messages=1, max_wait=0.05)
        assert [message async for message in batch] == []
    assert batch.error is None
