import argparse
import asyncio
import sys
import time

import nats

DEFAULT_FLUSH_TIMEOUT = 30
DEFAULT_NUM_MSGS = 100000
DEFAULT_MSG_SIZE = 16
DEFAULT_BATCH_SIZE = 100
HASH_MODULO = 1000


def show_usage():
    message = """
Usage: sub_perf_messages [options]

options:
    -n COUNT                         Messages to expect (default: 100000}
    -S SUBJECT                       Send subject (default: (test)
    """
    print(message)


def show_usage_and_die():
    show_usage()
    sys.exit(1)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--count", default=DEFAULT_NUM_MSGS, type=int)
    parser.add_argument("-S", "--subject", default="test")
    parser.add_argument("--servers", default=[], action="append")
    args = parser.parse_args()

    servers = args.servers
    if len(args.servers) < 1:
        servers = ["nats://127.0.0.1:4222"]

    # Make sure we're connected to a server first...
    try:
        nc = await nats.connect(servers, allow_reconnect=False)
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}")
        show_usage_and_die()

    received = 0
    start = None

    sub = await nc.subscribe(args.subject)

    print(f"Waiting for {args.count} messages on [{args.subject}]...")
    try:
        # Additional roundtrip with server to ensure everything has been
        # processed by the server already.
        await nc.flush()
    except nats.aio.errors.ErrTimeout:
        print(f"Server flush timeout after {DEFAULT_FLUSH_TIMEOUT}")

    async for msg in sub.messages:
        received += 1

        # Measure time from when we get the first message.
        if received == 1:
            start = time.monotonic()
        if (received % HASH_MODULO) == 0:
            sys.stdout.write("*")
            sys.stdout.flush()

        if received >= args.count:
            break

    elapsed = time.monotonic() - start
    print("\nTest completed : {} msgs/sec sent".format(args.count / elapsed))

    print("Received {} messages ({} msgs/sec)".format(received, received / elapsed))
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
