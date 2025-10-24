#!/usr/bin/env python3
"""NATS Reply Example.

Listens for requests on a subject and automatically replies with a predefined response.

Usage:
    python nats-rply.py [-s server] [-creds file] [-nkey file] [-t] [-q queue] <subject> <response>

Examples:
    python nats-rply.py help "I can help!"
    python nats-rply.py -s nats://demo.nats.io:4222 help "I can help!"
    python nats-rply.py -q workers help "I can help!"
"""

import argparse
import asyncio
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
    """Listen for requests and send replies."""
    parser = argparse.ArgumentParser(
        description="NATS Reply",
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
        "-q",
        "--queue",
        default="NATS-RPLY-22",
        help="Queue group name (default: NATS-RPLY-22)",
    )
    parser.add_argument(
        "subject",
        help="Subject to listen on",
    )
    parser.add_argument(
        "response",
        help="Response message to send",
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

    try:
        # Connect to NATS
        client = await connect(
            args.server,
            token=token,
            user=user,
            password=password,
            nkey_seed=nkey_seed,
        )

        print(f"Listening on [{args.subject}] in queue group [{args.queue}]")

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

                    # Log the received request
                    if args.timestamp:
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        print(f"[#{count} {timestamp}] Received request on [{msg.subject}]: {msg.data.decode()}")
                    else:
                        print(f"[#{count}] Received request on [{msg.subject}]: {msg.data.decode()}")

                    # Send the reply if a reply subject is provided
                    if msg.reply_to:
                        await client.publish(msg.reply_to, args.response.encode())
                        if args.timestamp:
                            timestamp = datetime.now().strftime("%H:%M:%S")
                            print(f"[#{count} {timestamp}] Sent reply: {args.response}")
                        else:
                            print(f"[#{count}] Sent reply: {args.response}")
                    else:
                        print(f"[#{count}] Warning: No reply subject in request", file=sys.stderr)

                except asyncio.TimeoutError:
                    # No message received, continue loop to check shutdown
                    continue
                except Exception as e:
                    print(f"Error processing request: {e}", file=sys.stderr)
                    break

        # Close the connection (drains pending messages)
        await client.close()
        print("Reply service stopped")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
