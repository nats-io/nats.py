"""A candidate succeeds only after its complete authenticated handshake."""

import asyncio
import json
from contextlib import asynccontextmanager

import pytest
from nats.client import ClientStatus, connect


@asynccontextmanager
async def peer(reply=b"PONG\r\n", expected_token=None):
    connections = []
    handlers = set()
    commands = asyncio.Queue()
    closed = asyncio.Event()

    async def handle(reader, writer):
        handlers.add(asyncio.current_task())
        connections.append(writer)
        try:
            writer.write(
                b'INFO {"server_id":"test","version":"1","go":"go1","headers":true,"host":"127.0.0.1","port":4222}\r\n'
            )
            await writer.drain()
            while line := await reader.readline():
                if line.startswith(b"CONNECT "):
                    command = json.loads(line[8:])
                    commands.put_nowait(command)
                    if expected_token is not None and command.get("auth_token") != expected_token:
                        writer.write(b"-ERR 'Authorization Violation'\r\n")
                        await writer.drain()
                        return
                elif line == b"PING\r\n":
                    if reply is None:
                        return
                    writer.write(reply)
                    await writer.drain()
        finally:
            writer.close()
            closed.set()

    listener = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    try:
        yield f"nats://127.0.0.1:{port}", commands, closed
    finally:
        listener.close()
        await listener.wait_closed()
        for writer in connections:
            writer.close()
        for task in handlers:
            task.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)


@pytest.mark.parametrize("reply", [b"-ERR 'Authorization Violation'\r\n", None, b"PING\r\n", b""])
async def test_full_handshake_failure_tries_next_seed(reply):
    async with peer(reply) as (bad, _, closed), peer() as (good, _, _):
        client = await connect([bad, good], timeout=0.05, no_randomize=True)
        try:
            assert client.status == ClientStatus.CONNECTED
            await asyncio.wait_for(closed.wait(), 1)
            await client.flush(timeout=1)
        finally:
            await client.close()


async def test_each_seed_uses_its_own_token_on_connect_and_reconnect():
    async with (
        peer(expected_token="alpha") as (first, first_commands, _),
        peer(expected_token="beta") as (second, second_commands, _),
    ):
        first = first.replace("nats://", "nats://alpha@")
        second = second.replace("nats://", "nats://beta@")
        client = await connect([first, second], reconnect_time_wait=0, no_randomize=True)
        try:
            assert (await first_commands.get())["auth_token"] == "alpha"
            # Keep only the second configured entry to force failover deterministically.
            client._server_pool = [client._server_pool[1]]
            await asyncio.wait_for(client.force_reconnect(), 1)
            assert (await second_commands.get())["auth_token"] == "beta"
            assert client.status == ClientStatus.CONNECTED
        finally:
            await client.close()


async def test_failed_seed_does_not_supply_credentials_to_next_seed():
    async with peer(expected_token="alpha") as (first, _, _), peer(expected_token="beta") as (second, commands, _):
        first = first.replace("nats://", "nats://wrong@")
        second = second.replace("nats://", "nats://beta@")
        client = await connect([first, second], timeout=1)
        try:
            assert (await commands.get())["auth_token"] == "beta"
        finally:
            await client.close()
