#!/usr/bin/env python3
"""NATS Request Example.

Sends a request message to a subject and waits for a reply.

Usage:
    python nats-req.py [-s server] [-creds file] [-nkey file] <subject> <msg>

Examples:
    python nats-req.py help "What is NATS?"
    python nats-req.py -s nats://demo.nats.io:4222 help "What is NATS?"
"""

import argparse
import asyncio
import sys

from nats.client import connect


async def main():
    """Send a request to NATS and wait for a reply."""
    parser = argparse.ArgumentParser(
        description="NATS Request",
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
        help="Subject to send request to",
    )
    parser.add_argument(
        "message",
        help="Request message",
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

    try:
        # Connect to NATS
        client = await connect(
            args.server,
            token=token,
            user=user,
            password=password,
            nkey_seed=nkey_seed,
        )

        print(f"Published [{args.subject}] : '{args.message}'")

        # Send request and wait for reply (2 second timeout)
        try:
            response = await client.request(args.subject, args.message.encode(), timeout=2.0)
            print(f"Received  [{response.subject}] : '{response.data.decode()}'")
        except asyncio.TimeoutError:
            print("Request timeout - no reply received", file=sys.stderr)
            sys.exit(1)

        # Close the connection
        await client.close()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
