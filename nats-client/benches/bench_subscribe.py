"""Benchmarks for NATS client subscription operations - measuring throughput."""

import pytest
from nats.client import connect
from nats.server import run


@pytest.mark.asyncio
@pytest.mark.parametrize("count,payload_size", [
    # 1k messages with power-of-2 payload sizes
    (1000, 0),
    (1000, 32),
    (1000, 64),
    (1000, 128),
    (1000, 256),
    (1000, 512),
    (1000, 1024),
    (1000, 2048),
    (1000, 4096),
    (1000, 8192),
    (1000, 16384),
    (1000, 32768),
    (1000, 65536),
    # 10k messages with power-of-2 payload sizes
    (10000, 0),
    (10000, 32),
    (10000, 64),
    (10000, 128),
    (10000, 256),
    (10000, 512),
    (10000, 1024),
    (10000, 2048),
    (10000, 4096),
    (10000, 8192),
    (10000, 16384),
    (10000, 32768),
    (10000, 65536),
    # 100k messages with power-of-2 payload sizes
    (100000, 0),
    (100000, 32),
    (100000, 64),
    (100000, 128),
    (100000, 256),
    (100000, 512),
    (100000, 1024),
    (100000, 2048),
    (100000, 4096),
    (100000, 8192),
    (100000, 16384),
    (100000, 32768),
    (100000, 65536),
    # 1M messages with power-of-2 payload sizes
    (1000000, 0),
    (1000000, 32),
    (1000000, 64),
    (1000000, 128),
    (1000000, 256),
    (1000000, 512),
    (1000000, 1024),
    (1000000, 2048),
    (1000000, 4096),
    (1000000, 8192),
    (1000000, 16384),
    (1000000, 32768),
    (1000000, 65536),
])
async def bench_subscribe_throughput(benchmark, count, payload_size):
    """Benchmark receiving messages - throughput test."""
    server = await run(port=0)
    client = await connect(server.client_url, timeout=1.0)
    payload = b"x" * payload_size

    try:
        sub = await client.subscribe("test.subject")
        for _ in range(count):
            await client.publish("test.subject", payload)
        await client.flush(timeout=120.0)

        async def run_receive():
            for _ in range(count):
                await sub.next()

        await benchmark(run_receive)
    finally:
        await client.close()
        await server.shutdown()
