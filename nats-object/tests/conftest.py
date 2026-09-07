"""Test fixtures for nats-object."""

from __future__ import annotations

import logging
import tempfile
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from nats.client import Client, connect
from nats.jetstream import JetStream
from nats.jetstream import new as new_jetstream
from nats.server import Server, run


@pytest.fixture(autouse=True)
def configure_logging() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s %(name)s: %(message)s",
        force=True,
    )


@pytest.fixture
def store_dir() -> Generator[str, None, None]:
    with tempfile.TemporaryDirectory() as path:
        yield path


@pytest_asyncio.fixture
async def server(store_dir: str) -> AsyncGenerator[Server, None]:
    async with await run(port=0, jetstream=True, store_dir=store_dir) as server:
        yield server


@pytest_asyncio.fixture
async def client(server: Server) -> AsyncGenerator[Client, None]:
    client = await connect(server.client_url)
    try:
        yield client
    finally:
        await client.close()


@pytest_asyncio.fixture
async def jetstream(client: Client) -> JetStream:
    return new_jetstream(client)
