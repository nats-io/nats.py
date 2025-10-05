"""Benchmarks for NATS protocol parsing operations."""

import asyncio

import pytest
from nats.client.protocol.message import parse, parse_headers


class MockReader:
    """Mock reader for testing protocol parsing."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    async def readline(self) -> bytes:
        """Read a line from the data."""
        if self.pos >= len(self.data):
            return b""

        start = self.pos
        while self.pos < len(self.data) and self.data[self.pos:self.pos + 1] != b"\n":
            self.pos += 1

        if self.pos < len(self.data):
            self.pos += 1

        return self.data[start:self.pos]

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes from the data."""
        if self.pos + n > len(self.data):
            raise asyncio.IncompleteReadError(
                self.data[self.pos:], n - (len(self.data) - self.pos)
            )

        result = self.data[self.pos:self.pos + n]
        self.pos += n
        return result


def bench_parse_msg(benchmark):
    """Benchmark parsing MSG protocol message."""
    msg_data = b"MSG test.subject 1 5\r\nhello\r\n"

    def run_parse():
        async def _parse():
            reader = MockReader(msg_data)
            return await parse(reader)
        return asyncio.run(_parse())

    benchmark(run_parse)


def bench_parse_hmsg(benchmark):
    """Benchmark parsing HMSG protocol message."""
    msg_data = b"HMSG test.subject 1 reply 44 49\r\nNATS/1.0\r\nContent-Type: application/json\r\n\r\nhello\r\n"

    def run_parse():
        async def _parse():
            reader = MockReader(msg_data)
            return await parse(reader)
        return asyncio.run(_parse())

    benchmark(run_parse)


def bench_parse_headers_simple(benchmark):
    """Benchmark parsing simple headers."""
    header_data = b"NATS/1.0\r\nContent-Type: application/json\r\n\r\n"

    def run_parse():
        return parse_headers(header_data)

    benchmark(run_parse)


def bench_parse_headers_with_status(benchmark):
    """Benchmark parsing headers with status code."""
    header_data = b"NATS/1.0 503 No Responders\r\n\r\n"

    def run_parse():
        return parse_headers(header_data)

    benchmark(run_parse)


def bench_parse_headers_multiple(benchmark):
    """Benchmark parsing multiple headers."""
    header_data = (
        b"NATS/1.0\r\n"
        b"Content-Type: application/json\r\n"
        b"X-Custom-Header: value1\r\n"
        b"X-Another-Header: value2\r\n"
        b"X-Multi-Value: a\r\n"
        b"X-Multi-Value: b\r\n"
        b"\r\n"
    )

    def run_parse():
        return parse_headers(header_data)

    benchmark(run_parse)
