#!/usr/bin/env python3
"""NATS Queue Subscriber Example.

Subscribes to a subject with a queue group for load-balanced message processing.
Multiple instances with the same queue group will share messages.

Usage:
    python nats-qsub.py [-s server] [-creds file] [-nkey file] [-t] <subject> <queue>

Examples:
    python nats-qsub.py hello workers
    python nats-qsub.py -s nats://demo.nats.io:4222 hello workers
    python nats-qsub.py -t hello workers  # with timestamps
"""

import argparse
import asyncio
import os
import signal
import sys
from datetime import datetime

from nats.client import connect

# Global flag for graceful shutdown
shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    """Handle interrupt signal for graceful shutdown."""
    print("\nShutting down...")
    shutdown_event.set()


async def main():
    """Subscribe to messages from NATS using a queue group."""
    parser = argparse.ArgumentParser(
        description="NATS Queue Subscriber",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-s",
        "--server",
        default="nats://localhost:4222",
        help="NATS server URL (default: nats://localhost:4222)",
    )
    parser.add_argument(
        "-creds",
        "--credentials",
        help="User credentials file",
    )
    parser.add_argument(
        "-nkey",
        "--nkey",
        help="NKey seed file",
    )
    parser.add_argument(
        "-t",
        "--timestamp",
        action="store_true",
        help="Display timestamps",
    )
    parser.add_argument(
        "subject",
        help="Subject to subscribe to",
    )
    parser.add_argument(
        "queue",
        help="Queue group name",
    )

    args = parser.parse_args()

    # Load credentials if provided
    token = None
    user = None
    password = None
    nkey_seed = None

    if args.credentials:
        with open(args.credentials) as f:
            token = f.read().strip()

    if args.nkey:
        with open(args.nkey) as f:
            nkey_seed = f.read().strip()

    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Get process ID for logging
    pid = os.getpid()

    try:
        # Connect to NATS
        client = await connect(
            args.server,
            token=token,
            user=user,
            password=password,
            nkey_seed=nkey_seed,
        )

        print(f"Listening on [{args.subject}] in queue group [{args.queue}] (PID: {pid})")

        # Subscribe to the subject with queue group
        subscription = await client.subscribe(args.subject, queue_group=args.queue)

        # Message counter
        count = 0

        async with subscription:
            while not shutdown_event.is_set():
                try:
                    # Wait for message with timeout to allow checking shutdown_event
                    msg = await asyncio.wait_for(subscription.next(), timeout=0.5)
                    count += 1

                    # Format output
                    if args.timestamp:
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        print(
                            f"[#{count} {timestamp}] Received on [{msg.subject}] "
                            f"Queue[{args.queue}] Pid[{pid}]: {msg.data.decode()}"
                        )
                    else:
                        print(
                            f"[#{count}] Received on [{msg.subject}] Queue[{args.queue}] Pid[{pid}]: {msg.data.decode()}"
                        )

                except asyncio.TimeoutError:
                    # No message received, continue loop to check shutdown
                    continue
                except Exception as e:
                    print(f"Error receiving message: {e}", file=sys.stderr)
                    break

        # Close the connection (drains pending messages)
        await client.close()
        print("Queue subscription closed")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
