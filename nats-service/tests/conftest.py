"""Test fixtures for nats-service."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from nats.client import Client, connect
from nats.server import Server, run


@pytest.fixture(autouse=True)
def configure_logging() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s %(name)s: %(message)s",
        force=True,
    )


@pytest_asyncio.fixture
async def server() -> AsyncGenerator[Server, None]:
    async with await run(port=0) as server:
        yield server


@pytest_asyncio.fixture
async def client(server: Server) -> AsyncGenerator[Client, None]:
    client = await connect(server.client_url)
    try:
        yield client
    finally:
        await client.close()
