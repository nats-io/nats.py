import logging
import sys
from collections.abc import AsyncGenerator

import pytest_asyncio
from nats.client import Client, connect
from nats.server import Server, run

# Configure logging to see debug messages
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", stream=sys.stdout
)


@pytest_asyncio.fixture
async def server() -> AsyncGenerator[Server, None]:
    """Fixture that provides an isolated NATS server for each test.

    Returns:
        The NATS server instance.
    """
    server = await run(port=0)
    try:
        yield server
    finally:
        await server.shutdown()


@pytest_asyncio.fixture
async def client(server: Server) -> AsyncGenerator[Client, None]:
    """Fixture that provides a client connected to an isolated server."""
    client = await connect(server.client_url, timeout=1.0)
    try:
        yield client
    finally:
        await client.close()
