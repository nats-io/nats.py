import argparse
import asyncio
import sys
import time
from random import randint

import nats

DEFAULT_FLUSH_TIMEOUT = 30
DEFAULT_NUM_MSGS = 100000
DEFAULT_MSG_SIZE = 16
DEFAULT_BATCH_SIZE = 100
HASH_MODULO = 1000

def show_usage():
    message = """
Usage: sub_perf [options]

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
    parser.add_argument('-n', '--count', default=DEFAULT_NUM_MSGS, type=int)
    parser.add_argument('-S', '--subject', default='test')
    parser.add_argument('--servers', default=[], action='append')
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

    async def handler(msg):
        nonlocal received
        nonlocal start
        received += 1

        # Measure time from when we get the first message.
        if received == 1:
            start = time.monotonic()
        if (received % HASH_MODULO) == 0:
            sys.stdout.write("*")
            sys.stdout.flush()

    await nc.subscribe(args.subject, cb=handler)

    print(f"Waiting for {args.count} messages on [{args.subject}]...")
    try:
        # Additional roundtrip with server to ensure everything has been
        # processed by the server already.
        await nc.flush()
    except nats.aio.errors.ErrTimeout:
        print(f"Server flush timeout after {DEFAULT_FLUSH_TIMEOUT}")

    while received < args.count:
        await asyncio.sleep(0.1)

    elapsed = time.monotonic() - start
    print("\nTest completed : {} msgs/sec sent".format(args.count/elapsed))

    print("Received {} messages ({} msgs/sec)".format(received, received/elapsed))
    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
