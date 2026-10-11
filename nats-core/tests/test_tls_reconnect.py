"""Reconnect must reject plaintext peers without abandoning the server pool."""

import asyncio
import ssl
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import pytest
from nats.client import ClientStatus, connect
from nats.client.errors import SecureConnectionRequiredError
from nats.server import run


@asynccontextmanager
async def plaintext_peer():
    received = asyncio.Queue()
    tasks = []
    writers = []

    async def handle(reader, writer):
        writers.append(writer)
        try:
            writer.write(b'INFO {"tls_required":false}\r\n')
            await writer.drain()
            received.put_nowait(await reader.read())
        finally:
            writer.close()

    def accept(reader, writer):
        tasks.append(asyncio.create_task(handle(reader, writer)))

    listener = await asyncio.start_server(accept, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    try:
        yield f"nats://127.0.0.1:{port}", received
    finally:
        listener.close()
        for writer in writers:
            writer.transport.abort()
        for task in tasks:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await listener.wait_closed()
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result


@pytest.mark.parametrize("recover", [True, False], ids=["secure-failover", "exhausted"])
@pytest.mark.parametrize("automatic", [True, False], ids=["disconnect", "force-reconnect"])
@pytest.mark.parametrize("callback_raises", [False, True], ids=["callback", "broken-callback"])
async def test_tls_reconnect_skips_plaintext_peer(recover, automatic, callback_raises):
    config = Path(__file__).parent / "configs" / "server_tls_upgrade.conf"
    ctx = ssl.create_default_context(cafile=str(Path(__file__).parent / "certs" / "ca.pem"))

    async with AsyncExitStack() as stack:
        seed = await stack.enter_async_context(await run(config_path=str(config), port=0))
        healthy = await stack.enter_async_context(await run(config_path=str(config), port=0)) if recover else None
        plaintext_url, received = await stack.enter_async_context(plaintext_peer())
        client = await stack.enter_async_context(
            await connect(
                seed.client_url,
                tls=ctx,
                user="test-user",
                password="test-password",
                timeout=1,
                reconnect_time_wait=0.01,
                reconnect_time_wait_max=0.01,
                reconnect_jitter=0,
                reconnect_max_attempts=2,
                no_randomize=True,
            )
        )
        # Isolate candidate handling from discovery: a plaintext peer comes
        # before a usable TLS peer, or is the only remaining candidate.
        client._server_pool = [plaintext_url] + ([healthy.client_url] if healthy is not None else [])
        errors = []

        def error_callback(error):
            errors.append(error)
            if callback_raises:
                raise RuntimeError("callback failure")

        client.add_error_callback(error_callback)
        if automatic:
            await seed.shutdown()
        else:
            await asyncio.wait_for(client.force_reconnect(), timeout=3)

        async with asyncio.timeout(3):
            while (
                client.stats().reconnects != int(recover)
                or client._reconnecting
                or (not recover and client.status != ClientStatus.CLOSED)
            ):
                await asyncio.sleep(0.01)

        assert client.status == (ClientStatus.CONNECTED if recover else ClientStatus.CLOSED)
        assert not client._reconnecting
        assert len(errors) == (1 if recover else 2)
        assert all(isinstance(error, SecureConnectionRequiredError) for error in errors)
        for _ in errors:
            # The rejected peer sees neither CONNECT nor the test credentials.
            assert await asyncio.wait_for(received.get(), timeout=1) == b""

        if recover:
            subscription = await client.subscribe("tls.failover")
            await client.publish("tls.failover", b"secure")
            assert (await subscription.next(timeout=1)).data == b"secure"
