#!/usr/bin/env python3

"""
NATS WebSocket proxy example.

Usage:
    python websocket_proxy_example.py [--proxy http://localhost:8888] [--proxy-user user] [--proxy-password pass]
"""

import asyncio
import argparse
import nats


async def main():
    """Connect to NATS WebSocket server with optional proxy"""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="NATS WebSocket Proxy Example")
    parser.add_argument("--server", default="ws://localhost:8080",
                       help="NATS WebSocket server URL (default: ws://localhost:8080)")
    parser.add_argument("--proxy", help="HTTP proxy URL (e.g., http://localhost:8888)")
    parser.add_argument("--proxy-user", help="Proxy username for authentication")
    parser.add_argument("--proxy-password", help="Proxy password for authentication")

    args = parser.parse_args()

    # Build connection options
    connect_options = {
        "servers": [args.server]
    }

    if args.proxy:
        connect_options["proxy"] = args.proxy
        print(f"Proxy: {args.proxy}")

        if args.proxy_user and args.proxy_password:
            connect_options["proxy_user"] = args.proxy_user
            connect_options["proxy_password"] = args.proxy_password
            print(f"Auth: {args.proxy_user}")
        elif args.proxy_user or args.proxy_password:
            print("Error: Both user and password required")
            return 1

    try:
        print(f"Connecting to {args.server}...")
        nc = await nats.connect(**connect_options)
        print("Connected")

        # Test pub/sub
        messages = []

        async def handler(msg):
            messages.append(msg.data.decode())

        await nc.subscribe("test", cb=handler)
        await nc.flush()

        # Send test message
        for i in range(10):
            await nc.publish("test", f"hello {i}".encode())
        await nc.flush()
        await asyncio.sleep(0.1)

        expected = [f"hello {i}" for i in range(10)]
        assert messages == expected, f"Expected {expected}, got {messages}"
        print(f"Success: All {len(messages)} messages received correctly")

        await nc.close()

    except Exception as e:
        print(f"Failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
