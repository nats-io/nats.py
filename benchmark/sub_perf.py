import argparse, sys
import asyncio
import time
from random import randint
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrTimeout

DEFAULT_FLUSH_TIMEOUT = 30
DEFAULT_NUM_MSGS = 100000
DEFAULT_MSG_SIZE = 16
DEFAULT_BATCH_SIZE = 100
HASH_MODULO = 1000

def show_usage():
    message = """
Usage: sub_perf [options]

options:
    -n COUNT                         Messages to send (default: 100000}
    -t SUBTYPE                       Subscription type to use. Valid choices are 'async','sync' (default: sync)
    -S SUBJECT                       Send subject (default: (test)
    """
    print(message)

def show_usage_and_die():
    show_usage()
    sys.exit(1)

@asyncio.coroutine
def main(loop):
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--count', default=DEFAULT_NUM_MSGS, type=int)
    parser.add_argument('-S', '--subject', default='test')
    parser.add_argument('-t', '--subtype', default='sync')
    parser.add_argument('--servers', default=[], action='append')
    args = parser.parse_args()

    servers = args.servers
    if len(args.servers) < 1:
        servers = ["nats://127.0.0.1:4222"]
    opts = { "servers": servers, "io_loop": loop, "allow_reconnect": False }

    # Make sure we're connected to a server first...
    nc = NATS()
    try:
        yield from nc.connect(**opts)
    except Exception as e:
        sys.stderr.write("ERROR: {0}".format(e))
        show_usage_and_die()

    received = 0
    start = None

    @asyncio.coroutine
    def handler(msg):
        nonlocal received
        nonlocal start
        received += 1

        # Measure time from when we get the first message.
        if received == 1:
            start = time.monotonic()
        if (received % HASH_MODULO) == 0:
            sys.stdout.write("*")
            sys.stdout.flush()

    if args.subtype == 'sync':
        yield from nc.subscribe(args.subject, cb=handler)
    elif args.subtype == 'async':
        yield from nc.subscribe_async(args.subject, cb=handler)
    else:
        sys.stderr.write("ERROR: Unsupported type of subscription {0}".format(e))
        show_usage_and_die()

    print("Waiting for {} messages on [{}]...".format(args.count, args.subject))
    try:
        # Additional roundtrip with server to ensure everything has been
        # processed by the server already.
        yield from nc.flush()
    except ErrTimeout:
        print("Server flush timeout after {0}".format(DEFAULT_FLUSH_TIMEOUT))

    while received < args.count:
        yield from asyncio.sleep(0.1, loop=loop)

    elapsed = time.monotonic() - start
    print("\nTest completed : {0} msgs/sec sent".format(args.count/elapsed))

    print("Received {0} messages ({1} msgs/sec)".format(received, received/elapsed))
    yield from nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
