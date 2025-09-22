#!/usr/bin/env python3
"""NATS benchmarking tool."""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass

from nats.client import Headers, connect


@dataclass
class BenchmarkResults:
    """Results from a benchmark run."""

    msg_count: int
    msg_bytes: int
    duration: float
    throughput: float
    avg_latency: float
    min_latency: float
    max_latency: float
    std_latency: float
    bytes_per_sec: float
    mb_per_sec: float

    def __str__(self) -> str:
        return (
            f"\nTest completed: {self.msg_count:,} messages, "
            f"{self.msg_bytes:,} bytes, {self.duration:.2f} seconds\n"
            f"  Throughput: {self.throughput:,.0f} msgs/sec, "
            f"{self.mb_per_sec:.2f} MB/sec\n"
            f"  Latency: (min/avg/max/std) = "
            f"{self.min_latency*1000:.2f}/"
            f"{self.avg_latency*1000:.2f}/"
            f"{self.max_latency*1000:.2f}/"
            f"{self.std_latency*1000:.2f} ms"
        )


async def run_pub_benchmark(
    *,
    url: str = "nats://localhost:4222",
    msg_count: int = 100_000,
    msg_size: int = 128,
    pub_subject: str = "test",
    headers: Headers | None = None,
) -> BenchmarkResults:
    """Run publisher benchmark."""

    # Connect to server
    nc = await connect(url)

    try:
        # Prepare payload
        payload = b"x" * msg_size

        # Track latencies
        latencies = []
        start_time = time.perf_counter()

        # Publish messages
        for _ in range(msg_count):
            msg_start = time.perf_counter()
            await nc.publish(pub_subject, payload, headers=headers)
            latencies.append(time.perf_counter() - msg_start)

        await nc.flush()

        duration = time.perf_counter() - start_time

        # Calculate stats
        total_bytes = msg_count * msg_size
        throughput = msg_count / duration
        bytes_per_sec = total_bytes / duration
        mb_per_sec = bytes_per_sec / (1024 * 1024)

        # Calculate latency stats
        min_latency = min(latencies)
        max_latency = max(latencies)
        avg_latency = sum(latencies) / len(latencies)
        variance = sum((latency - avg_latency)**2
                       for latency in latencies) / len(latencies)
        std_latency = variance**0.5

        return BenchmarkResults(
            msg_count=msg_count,
            msg_bytes=total_bytes,
            duration=duration,
            throughput=throughput,
            avg_latency=avg_latency,
            min_latency=min_latency,
            max_latency=max_latency,
            std_latency=std_latency,
            bytes_per_sec=bytes_per_sec,
            mb_per_sec=mb_per_sec,
        )

    finally:
        await nc.close()


async def run_sub_benchmark(
    *,
    url: str = "nats://localhost:4222",
    msg_count: int = 100_000,
    sub_subject: str = "test"
) -> BenchmarkResults:
    """Run subscriber benchmark."""

    # Connect to server
    nc = await connect(url)
    received = 0
    first_msg_time = 0.0
    last_msg_time = 0.0
    total_bytes = 0
    latencies = []

    try:
        # Create subscription
        sub = await nc.subscribe(sub_subject)
        start_time = time.perf_counter()

        # Receive messages
        async for msg in sub:
            msg_time = time.perf_counter()
            if received == 0:
                first_msg_time = msg_time

            received += 1
            total_bytes += len(msg.data)
            latencies.append(msg_time - start_time)

            if received >= msg_count:
                last_msg_time = msg_time
                break

        duration = last_msg_time - first_msg_time

        # Calculate stats
        throughput = received / duration
        bytes_per_sec = total_bytes / duration
        mb_per_sec = bytes_per_sec / (1024 * 1024)

        # Calculate latency stats
        min_latency = min(latencies)
        max_latency = max(latencies)
        avg_latency = sum(latencies) / len(latencies)
        variance = sum((latency - avg_latency)**2
                       for latency in latencies) / len(latencies)
        std_latency = variance**0.5

        return BenchmarkResults(
            msg_count=received,
            msg_bytes=total_bytes,
            duration=duration,
            throughput=throughput,
            avg_latency=avg_latency,
            min_latency=min_latency,
            max_latency=max_latency,
            std_latency=std_latency,
            bytes_per_sec=bytes_per_sec,
            mb_per_sec=mb_per_sec,
        )

    finally:
        await nc.close()


async def run_pubsub_benchmark(
    *,
    url: str = "nats://localhost:4222",
    msg_count: int = 100_000,
    msg_size: int = 128,
    subject: str = "test",
    headers: Headers | None = None,
) -> tuple[BenchmarkResults, BenchmarkResults]:
    """Run combined publisher/subscriber benchmark."""

    # Start subscriber first
    sub_task = asyncio.create_task(
        run_sub_benchmark(url=url, msg_count=msg_count, sub_subject=subject)
    )

    # Small delay to ensure subscriber is ready
    await asyncio.sleep(0.1)

    # Run publisher
    pub_results = await run_pub_benchmark(
        url=url,
        msg_count=msg_count,
        msg_size=msg_size,
        pub_subject=subject,
        headers=headers
    )

    # Wait for subscriber to finish
    sub_results = await sub_task

    return pub_results, sub_results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="NATS benchmarking tool")
    parser.add_argument(
        "--url", default="nats://localhost:4222", help="NATS server URL"
    )
    parser.add_argument(
        "--msgs",
        type=int,
        default=100_000,
        help="Number of messages to publish"
    )
    parser.add_argument(
        "--size", type=int, default=128, help="Size of the message payload"
    )
    parser.add_argument(
        "--subject", default="test", help="Subject to use for messages"
    )
    parser.add_argument(
        "--pub", action="store_true", help="Run publisher benchmark"
    )
    parser.add_argument(
        "--sub", action="store_true", help="Run subscriber benchmark"
    )
    parser.add_argument(
        "--headers", type=int, help="Number of headers to add to messages"
    )

    args = parser.parse_args()

    # Default to pub/sub if neither specified
    if not args.pub and not args.sub:
        args.pub = True
        args.sub = True

    # Create headers if requested
    headers = None
    if args.headers:
        headers = Headers({
            f"key{i}": f"value{i}"
            for i in range(args.headers)
        })

    async def run():
        if args.pub and args.sub:
            sys.stdout.write(
                f"\nStarting pub/sub benchmark [msgs={args.msgs:,}, size={args.size:,} B]\n"
            )
            pub_results, sub_results = await run_pubsub_benchmark(
                url=args.url,
                msg_count=args.msgs,
                msg_size=args.size,
                subject=args.subject,
                headers=headers
            )
            sys.stdout.write(f"\nPublisher results: {pub_results}\n")
            sys.stdout.write(f"\nSubscriber results: {sub_results}\n")

        elif args.pub:
            sys.stdout.write(
                f"\nStarting publisher benchmark [msgs={args.msgs:,}, size={args.size:,} B]\n"
            )
            results = await run_pub_benchmark(
                url=args.url,
                msg_count=args.msgs,
                msg_size=args.size,
                pub_subject=args.subject,
                headers=headers
            )
            sys.stdout.write(f"\nResults: {results}\n")

        elif args.sub:
            sys.stdout.write(
                f"\nStarting subscriber benchmark [msgs={args.msgs:,}]\n"
            )
            results = await run_sub_benchmark(
                url=args.url, msg_count=args.msgs, sub_subject=args.subject
            )
            sys.stdout.write(f"\nResults: {results}\n")

    asyncio.run(run())


if __name__ == "__main__":
    main()
