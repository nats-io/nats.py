"""URL credentials override options and travel with their discovery lineage."""

import asyncio
import contextlib
import json
from contextlib import asynccontextmanager

import pytest
from nats.client import _server_credentials, connect


@pytest.mark.parametrize(
    "url,expected",
    [
        ("nats://al%40ice:p%3Ass@host:4222", (None, "al@ice", "p:ss")),
        ("nats://t%40k@host:4222", ("t@k", None, None)),
        ("nats://host:4222", ("option-token", "option-user", "option-password")),
        ("nats://u:%2540@host:4222", (None, "u", "%40")),
        ("nats://@host:4222", ("", None, None)),
        ("nats://:pass@host:4222", (None, "", "pass")),
    ],
)
def test_credentials_decode_once_and_replace_option_family(url, expected):
    assert _server_credentials(url, "option-token", "option-user", "option-password") == expected


@asynccontextmanager
async def peer(*, peers=(), single_use=False):
    frames = asyncio.Queue()
    tasks = set()
    connections = 0

    async def serve(reader, writer):
        nonlocal connections
        tasks.add(asyncio.current_task())
        connections += 1
        try:
            if single_use and connections > 1:
                writer.write(b"-ERR 'Maximum Connections Exceeded'\r\n")
                await writer.drain()
                return
            info = dict(
                server_id="test",
                version="2.14.0",
                go="go1",
                host="localhost",
                port=4222,
                headers=True,
                nonce="nonce",
                connect_urls=list(peers),
            )
            writer.write(b"INFO " + json.dumps(info).encode() + b"\r\n")
            await writer.drain()
            while line := await reader.readline():
                if line.startswith(b"CONNECT "):
                    frames.put_nowait(json.loads(line[8:]))
                elif line == b"PING\r\n":
                    writer.write(b"PONG\r\n")
                    await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            tasks.discard(asyncio.current_task())

    listener = await asyncio.start_server(serve, "127.0.0.1", 0)
    try:
        yield listener.sockets[0].getsockname()[1], frames
    finally:
        listener.close()
        await listener.wait_closed()
        for task in list(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_url_auth_overrides_nkey_on_connect_and_reconnect():
    def unwanted(*args):
        raise AssertionError("URL credentials should suppress NKey")

    async with peer() as (port, frames):
        async with await connect(
            f"nats://al%40ice:p%3Ass@127.0.0.1:{port}", token="stale", nkey=(unwanted, unwanted)
        ) as client:
            for reconnect in (False, True):
                if reconnect:
                    await client.force_reconnect()
                frame = await asyncio.wait_for(frames.get(), 1)
                assert frame["user"] == "al@ice"
                assert frame["pass"] == "p:ss"
                assert "auth_token" not in frame
                assert "nkey" not in frame


async def test_nkey_is_initialized_when_reconnecting_to_seed_without_url_credentials():
    from unittest.mock import Mock

    public_key = Mock(return_value="public-key")
    sign = Mock(return_value=b"signed-nonce")
    async with peer() as (second_port, second_frames), peer(single_use=True) as (first_port, first_frames):
        async with await connect(
            [f"nats://token@127.0.0.1:{first_port}", f"nats://127.0.0.1:{second_port}"],
            nkey=(public_key, sign),
            no_randomize=True,
        ) as client:
            first = await asyncio.wait_for(first_frames.get(), 5)
            assert first["auth_token"] == "token"
            assert "nkey" not in first
            public_key.assert_not_called()
            await asyncio.wait_for(client.force_reconnect(), 5)
            second = await asyncio.wait_for(second_frames.get(), 5)
            assert second["nkey"] == "public-key"
            assert second["sig"] == "signed-nonce"
            assert "auth_token" not in second
            public_key.assert_called_once()
            sign.assert_called_once_with("nonce")


async def test_discovered_peer_inherits_url_credentials_and_nkey_suppression():
    def unwanted(*args):
        raise AssertionError("discovered peer must retain URL authentication")

    async with peer() as (port, frames):
        async with peer(peers=[f"127.0.0.1:{port}"], single_use=True) as (seed_port, seed_frames):
            async with await connect(
                f"nats://original@127.0.0.1:{seed_port}", nkey=(unwanted, unwanted), no_randomize=True
            ) as client:
                await asyncio.wait_for(seed_frames.get(), 5)
                await asyncio.wait_for(client.force_reconnect(), 5)
                frame = await asyncio.wait_for(frames.get(), 5)
                assert frame["auth_token"] == "original"
                assert "user" not in frame and "pass" not in frame and "nkey" not in frame
