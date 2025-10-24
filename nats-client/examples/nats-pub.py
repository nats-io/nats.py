#!/usr/bin/env python3
"""NATS Publisher Example.

Publishes a message to a specified subject on a NATS server.

Usage:
    python nats-pub.py [-s server] [-creds file] [-nkey file] <subject> <msg>

Examples:
    python nats-pub.py hello "world"
    python nats-pub.py -s nats://demo.nats.io:4222 hello "world"
    python nats-pub.py -creds ~/.nats/creds hello "world"
"""

import argparse
import asyncio
import sys

from nats.client import connect


async def main():
    """Publish a message to NATS."""
    parser = argparse.ArgumentParser(
        description="NATS Publisher",
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
        "subject",
        help="Subject to publish to",
    )
    parser.add_argument(
        "message",
        help="Message to publish",
    )

    args = parser.parse_args()

    # Load credentials if provided
    token = None
    user = None
    password = None
    nkey_seed = None

    if args.credentials:
        # For simplicity, we'll just support token in credentials file
        # A full implementation would parse JWT credentials
        with open(args.credentials) as f:
            token = f.read().strip()

    if args.nkey:
        with open(args.nkey) as f:
            nkey_seed = f.read().strip()

    try:
        # Connect to NATS
        client = await connect(
            args.server,
            token=token,
            user=user,
            password=password,
            nkey_seed=nkey_seed,
        )

        # Publish the message
        await client.publish(args.subject, args.message.encode())
        await client.flush()

        print(f"Published [{args.subject}] : '{args.message}'")

        # Close the connection
        await client.close()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
