"""Only eligible INFOs contribute peers, including failed initial attempts."""

import asyncio
import contextlib
import json
from contextlib import asynccontextmanager
from typing import cast

import pytest
from nats.client import connect
from nats.client.connection import Connection, needs_post_pong_info, read_post_pong_info


@asynccontextmanager
async def peer(*, version="2.14.0", peers=None, reject=False, post_info=None, post_error=None, close_after_pong=False):
    tasks = set()

    async def serve(reader, writer):
        tasks.add(asyncio.current_task())
        info = dict(
            server_id="fake",
            version=version,
            go="go1",
            host="localhost",
            port=4222,
            headers=True,
            connect_urls=peers or [],
        )
        try:
            writer.write(b"INFO " + json.dumps(info).encode() + b"\r\n")
            await writer.drain()
            first = True
            while line := await reader.readline():
                if line == b"PING\r\n":
                    if reject:
                        writer.write(b"-ERR 'Authorization Violation'\r\n")
                        await writer.drain()
                        return
                    writer.write(b"PONG\r\n")
                    if first and post_error is not None:
                        writer.write(b"-ERR '" + post_error.encode() + b"'\r\n")
                    if first and post_info is not None:
                        writer.write(b"INFO " + json.dumps(info | {"connect_urls": post_info}).encode() + b"\r\n")
                    await writer.drain()
                    first = False
                    if close_after_pong:
                        return
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            tasks.discard(asyncio.current_task())

    listener = await asyncio.start_server(serve, "127.0.0.1", 0)
    try:
        yield f"nats://127.0.0.1:{listener.sockets[0].getsockname()[1]}"
    finally:
        listener.close()
        await listener.wait_closed()
        for task in list(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.fixture
def pretend_upgrade(monkeypatch):
    # Exercise the trust boundary independently from the TLS transport tests.
    import nats.client as module

    establish = module.establish_connection

    async def upgraded(*args, **kwargs):
        connection, info, _ = await establish(*args, **kwargs)
        return connection, info, True

    monkeypatch.setattr(module, "establish_connection", upgraded)


async def test_rejected_plaintext_seed_still_leads_to_peer():
    async with peer() as destination:
        async with peer(peers=[destination.removeprefix("nats://")], reject=True) as seed:
            async with await connect(seed, no_randomize=True) as client:
                assert client._last_server is not None
                assert client._last_server.url == destination
                assert all(server.reconnects == 0 for server in client._server_pool)


@pytest.mark.parametrize("version", ["2.11.0", "2.14.0", "invalid"])
async def test_upgrade_never_uses_plaintext_discovery(pretend_upgrade, version):
    async with peer(version=version, peers=["untrusted:4222"]) as seed:
        async with await connect(seed, timeout=0.02) as client:
            assert [server.url for server in client._server_pool] == [seed]
            assert client.server_info is not None
            assert not client.server_info.connect_urls


async def test_upgrade_waits_for_authenticated_info(pretend_upgrade):
    async with peer(peers=["old:4222"], post_info=["trusted:4222"]) as seed:
        async with await connect(seed, timeout=0.1) as client:
            assert client.server_info is not None
            assert client.server_info.connect_urls == ["trusted:4222"]
            assert all("old:" not in server.url for server in client._server_pool)
            assert any(server.url == "tls://trusted:4222" for server in client._server_pool)


async def test_disconnect_during_post_pong_wait_fails_attempt(pretend_upgrade):
    async with peer(close_after_pong=True) as seed:
        with pytest.raises(ConnectionError, match="post-PONG"):
            await connect(seed, timeout=0.1)


async def test_ignore_discovery_skips_post_pong_wait(pretend_upgrade):
    async with peer() as seed:
        async with await asyncio.wait_for(connect(seed, timeout=30, ignore_discovered_servers=True), 5) as client:
            assert len(client._server_pool) == 1


async def test_post_pong_wait_answers_ping_and_has_independent_timeout():
    class Stream:
        count = 0
        writes = []

        async def readline(self):
            self.count += 1
            if self.count == 1:
                return b"PING\r\n"
            await asyncio.Event().wait()

        async def write(self, data):
            self.writes.append(data)

    stream = Stream()
    assert await read_post_pong_info(cast(Connection, stream), timeout=0.02) is None
    assert stream.writes == [b"PONG\r\n"]


@pytest.mark.parametrize(
    "version,wait", [("2.11.9", False), ("2.12.0", True), ("v2.14.0", True), ("3.0.0-beta", True), ("bad", False)]
)
def test_server_version_only_controls_wait(version, wait):
    assert needs_post_pong_info(version) is wait


async def test_initial_post_pong_error_is_retained(pretend_upgrade):
    from nats.client.errors import PermissionsViolationError

    async with peer(post_info=["trusted:4222"], post_error="Permissions Violation for Publish to x") as seed:
        async with await connect(seed, timeout=5) as client:
            assert isinstance(client.last_error, PermissionsViolationError)
            assert client.server_info is not None
            assert client.server_info.connect_urls == ["trusted:4222"]
            await client.flush(timeout=5)
