"""Test fixtures for nats-jetstream."""

import logging
import tempfile
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from nats.client import Client, connect
from nats.jetstream import JetStream
from nats.jetstream import new as new_jetstream
from nats.server import Server, run


@pytest.fixture(autouse=True)
def configure_logging():
    """Configure logging for all tests."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s %(name)s: %(message)s",
        force=True,
    )


@pytest.fixture
def store_dir() -> Generator[str, None, None]:
    """Fixture that provides a unique temporary directory for NATS store.

    Returns:
        Path to the temporary directory.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest_asyncio.fixture
async def server(store_dir: str) -> AsyncGenerator[Server, None]:
    """Fixture that provides an isolated NATS server for each test.

    Args:
        store_dir: The directory to use for JetStream storage.

    Returns:
        The NATS server instance.
    """
    async with await run(port=0, jetstream=True, store_dir=store_dir) as server:
        yield server


@pytest_asyncio.fixture
async def client(server: Server) -> AsyncGenerator[Client, None]:
    """Fixture that provides a NATS client connected to the server.

    Args:
        server: The NATS server to connect to.

    Returns:
        The connected NATS client.
    """
    # Try to connect and verify JetStream is ready
    client = None
    try:
        client = await connect(server.client_url)
        yield client
    finally:
        if client:
            await client.close()


@pytest_asyncio.fixture
async def jetstream(client: Client) -> JetStream:
    """Fixture that provides a JetStream instance.

    Args:
        client: The NATS client to use.

    Returns:
        A JetStream instance.
    """
    return new_jetstream(client)
