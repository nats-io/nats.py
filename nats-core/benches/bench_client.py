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


REQUEST_SIZES = [1, 16, 128, 1024, 8192]

# The latency bench measures a single round-trip and lets pytest-benchmark
# repeat it across rounds, so no count is needed. The throughput bench drives a
# fixed number of requests through a bounded in-flight window and measures how
# fast they all complete; an unbounded gather starves under its own backlog.
THROUGHPUT_COUNT = 10_000
THROUGHPUT_INFLIGHT = 100


def _start_responder(subject):
    """Spin up a server, client, and an echo responder on ``subject``."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    server = loop.run_until_complete(run(port=0))
    client = loop.run_until_complete(connect(server.client_url))

    async def start():
        sub = await client.subscribe(subject)

        async def respond():
            async for msg in sub.messages:
                if msg.reply:
                    await client.publish(msg.reply, msg.data)

        return sub, asyncio.ensure_future(respond())

    sub, responder = loop.run_until_complete(start())
    return (loop, server, client, sub, responder), {}


def _stop_responder(loop, server, client, sub, responder):
    """Tear down the responder, client, server, and event loop."""

    async def stop():
        responder.cancel()
        try:
            await responder
        except asyncio.CancelledError:
            pass

    loop.run_until_complete(stop())
    loop.run_until_complete(client.close())
    loop.run_until_complete(server.shutdown())
    loop.close()
    asyncio.set_event_loop(None)


@pytest.mark.parametrize("size", REQUEST_SIZES)
def test_bench_request_latency(benchmark, size):
    """Benchmark a single request/reply round-trip (latency)."""
    subject = "bench.request"
    payload = b"x" * size

    # Set the server up once; pytest-benchmark repeats just the round-trip,
    # giving a per-request latency distribution instead of one aggregate.
    (loop, server, client, sub, responder), _ = _start_responder(subject)

    def one_request():
        loop.run_until_complete(client.request(subject, payload, timeout=5.0))

    benchmark.extra_info["message_size"] = size

    try:
        benchmark(one_request)
    finally:
        _stop_responder(loop, server, client, sub, responder)


@pytest.mark.parametrize("size", REQUEST_SIZES)
def test_bench_request_throughput(benchmark, size):
    """Benchmark concurrent in-flight request/reply (throughput bound)."""
    subject = "bench.request"
    payload = b"x" * size

    def execute(loop, server, client, sub, responder):
        async def request_n():
            sem = asyncio.Semaphore(THROUGHPUT_INFLIGHT)

            async def one():
                async with sem:
                    await client.request(subject, payload, timeout=5.0)

            await asyncio.gather(*(one() for _ in range(THROUGHPUT_COUNT)))

        loop.run_until_complete(request_n())

    benchmark.extra_info["message_size"] = size
    benchmark.extra_info["message_count"] = THROUGHPUT_COUNT

    return benchmark.pedantic(
        execute,
        setup=lambda: _start_responder(subject),
        teardown=_stop_responder,
        iterations=1,
        rounds=1,
    )
