"""Plaintext protocol data must not cross the TLS upgrade boundary."""

import asyncio
import ssl
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from nats.client import connect


@asynccontextmanager
async def tls_upgrade_server(plaintext_on=None, plaintext=b""):
    certs = Path(__file__).parent / "certs"
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(certs / "server-cert.pem", certs / "server-key.pem")
    client_ctx = ssl.create_default_context(cafile=str(certs / "ca.pem"))
    writers = []
    tasks = []
    tls_connections = []

    async def handle(reader, writer):
        writers.append(writer)
        attempt = len(writers)
        try:
            info = (
                b'INFO {"server_id":"test","version":"2.0.0","go":"go1.20",'
                b'"host":"127.0.0.1","port":4222,"headers":true,"tls_required":true}\r\n'
            )
            # One write puts the injected operations in the plaintext read-ahead
            # buffer alongside INFO, before the real TLS handshake starts.
            writer.write(info + (plaintext if attempt == plaintext_on else b""))
            await writer.drain()
            await writer.start_tls(server_ctx, ssl_handshake_timeout=2)
            tls_connections.append(attempt)
            while line := await reader.readline():
                if line == b"PING\r\n":
                    writer.write(b"PONG\r\n")
                    await writer.drain()
                elif line.startswith(b"SUB "):
                    _, subject, sid = line.split()
                    writer.write(b"MSG " + subject + b" " + sid + b" 7\r\nTRUSTED\r\n")
                    await writer.drain()
        except (ConnectionError, TimeoutError):
            # Rejecting the plaintext suffix closes the peer before TLS.
            pass
        finally:
            writer.close()

    def accept(reader, writer):
        tasks.append(asyncio.create_task(handle(reader, writer)))

    listener = await asyncio.start_server(accept, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    try:
        yield f"tls://localhost:{port}", client_ctx, tls_connections
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


@pytest.mark.parametrize(
    "plaintext",
    [
        b"PONG\r\n",
        b'INFO {"connect_urls":["injected.invalid:4222"]}\r\n',
        b"PONG\r\nMSG test.subject 1 6\r\nFORGED\r\n",
        b"P",
    ],
    ids=["pong", "info", "message", "partial-operation"],
)
async def test_tls_upgrade_rejects_plaintext_after_info(plaintext):
    async with tls_upgrade_server(plaintext_on=1, plaintext=plaintext) as (url, ctx, tls_connections):
        client = None
        try:
            with pytest.raises(ConnectionError, match="Unexpected plaintext data after INFO"):
                client = await connect(url, tls=ctx, timeout=2, allow_reconnect=False)
            assert tls_connections == []
        finally:
            if client is not None:
                await client.close()


async def test_tls_upgrade_accepts_authenticated_messages():
    async with tls_upgrade_server() as (url, ctx, tls_connections):
        async with await connect(url, tls=ctx, timeout=2, allow_reconnect=False) as client:
            subscription = await client.subscribe("test.subject")
            message = await subscription.next(timeout=2)
            assert message.data == b"TRUSTED"
            assert tls_connections == [1]


async def test_tls_reconnect_rejects_plaintext_message_for_existing_subscription():
    plaintext = b"PONG\r\nMSG test.subject 1 6\r\nFORGED\r\n"
    async with tls_upgrade_server(plaintext_on=2, plaintext=plaintext) as (url, ctx, tls_connections):
        async with await connect(
            url,
            tls=ctx,
            timeout=2,
            reconnect_timeout=2,
            reconnect_time_wait=0.01,
            reconnect_time_wait_max=0.01,
            reconnect_jitter=0,
            reconnect_max_attempts=2,
            no_randomize=True,
        ) as client:
            subscription = await client.subscribe("test.subject")
            assert (await subscription.next(timeout=2)).data == b"TRUSTED"

            await asyncio.wait_for(client.force_reconnect(), timeout=5)

            # The poisoned attempt must fail; only a fresh TLS connection may
            # restore the subscription and deliver its next message.
            assert (await subscription.next(timeout=2)).data == b"TRUSTED"
            assert tls_connections == [1, 3]
            assert client.stats().in_messages == 2
