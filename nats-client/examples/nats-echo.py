#!/usr/bin/env python3
"""NATS Echo Service Example.

Implements an echo service that replies to requests with the same message content.
Also provides a status endpoint that returns service information.

Usage:
    python nats-echo.py [-s server] [-creds file] [-nkey file] [-t] [-id service_id] <subject>

Examples:
    python nats-echo.py echo
    python nats-echo.py -s nats://demo.nats.io:4222 echo
    python nats-echo.py -id my-echo-1 -t echo
"""

import argparse
import asyncio
import json
import platform
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
    """Run the echo service."""
    parser = argparse.ArgumentParser(
        description="NATS Echo Service",
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
        "-id",
        "--service-id",
        default="nats-echo",
        help="Service identifier (default: nats-echo)",
    )
    parser.add_argument(
        "subject",
        help="Subject to listen on for echo requests",
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

    # Service info
    service_info = {
        "id": args.service_id,
        "subject": args.subject,
        "platform": platform.platform(),
        "python_version": platform.python_version(),
    }

    try:
        # Connect to NATS
        client = await connect(
            args.server,
            token=token,
            user=user,
            password=password,
            nkey_seed=nkey_seed,
        )

        print(f"Echo service '{args.service_id}' listening on [{args.subject}]")
        print(f"Status available on [{args.subject}.status]")

        # Subscribe to the echo subject (with queue group for load balancing)
        echo_subscription = await client.subscribe(args.subject, queue_group="echo-service")

        # Subscribe to the status subject (without queue group, all instances respond)
        status_subject = f"{args.subject}.status"
        status_subscription = await client.subscribe(status_subject)

        # Message counters
        echo_count = 0
        status_count = 0

        async def handle_echo():
            """Handle echo requests."""
            nonlocal echo_count
            async with echo_subscription:
                while not shutdown_event.is_set():
                    try:
                        msg = await asyncio.wait_for(echo_subscription.next(), timeout=0.5)
                        echo_count += 1

                        if args.timestamp:
                            timestamp = datetime.now().strftime("%H:%M:%S")
                            print(f"[#{echo_count} {timestamp}] Echo request: {msg.data.decode()}")
                        else:
                            print(f"[#{echo_count}] Echo request: {msg.data.decode()}")

                        # Echo back the message
                        if msg.reply_to:
                            await client.publish(msg.reply_to, msg.data)

                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        print(f"Error handling echo request: {e}", file=sys.stderr)
                        break

        async def handle_status():
            """Handle status requests."""
            nonlocal status_count
            async with status_subscription:
                while not shutdown_event.is_set():
                    try:
                        msg = await asyncio.wait_for(status_subscription.next(), timeout=0.5)
                        status_count += 1

                        if args.timestamp:
                            timestamp = datetime.now().strftime("%H:%M:%S")
                            print(f"[#{status_count} {timestamp}] Status request")
                        else:
                            print(f"[#{status_count}] Status request")

                        # Send status information
                        if msg.reply_to:
                            status_response = {
                                **service_info,
                                "echo_count": echo_count,
                                "status_count": status_count,
                            }
                            await client.publish(msg.reply_to, json.dumps(status_response).encode())

                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        print(f"Error handling status request: {e}", file=sys.stderr)
                        break

        # Run both handlers concurrently
        await asyncio.gather(
            handle_echo(),
            handle_status(),
        )

        # Close the connection
        await client.close()
        print("Echo service stopped")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
