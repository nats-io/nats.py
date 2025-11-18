"""Benchmarks for NATS client operations."""

import asyncio

import pytest
from nats.client import connect
from nats.server import run


@pytest.mark.parametrize(
    "size",
    [
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
        2048,
        4096,
        8192,
        16384,
        32768,
    ],
)
def test_bench_publish(benchmark, size):
    """Benchmark publish with various payload sizes."""
    subject = "bench.publish"
    payload = b"x" * size

    # Adjust count based on message size to keep total data volume consistent
    # Target ~10MB total per benchmark run
    target_bytes = 10 * 1024 * 1024
    count = max(1, target_bytes // max(1, size))

    def setup():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server = loop.run_until_complete(run(port=0))
        client = loop.run_until_complete(connect(server.client_url))
        return ((loop, server, client), {})

    def execute(loop, server, client):
        async def publish_n():
            for _ in range(count):
                await client.publish(subject, payload)

        loop.run_until_complete(publish_n())

    def teardown(loop, server, client):
        loop.run_until_complete(client.close())
        loop.run_until_complete(server.shutdown())
        loop.close()
        asyncio.set_event_loop(None)

    benchmark.extra_info["message_size"] = size
    benchmark.extra_info["message_count"] = count

    result = benchmark.pedantic(execute, setup=setup, teardown=teardown, iterations=1, rounds=1)
    return result
