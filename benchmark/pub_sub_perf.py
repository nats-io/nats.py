import argparse
import asyncio
import sys
import time
from random import randint

import nats

try:
  import uvloop
  asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except:
  pass

DEFAULT_FLUSH_TIMEOUT = 30
DEFAULT_NUM_MSGS = 100000
DEFAULT_MSG_SIZE = 16
DEFAULT_BATCH_SIZE = 100
HASH_MODULO = 1000

def show_usage():
    message = """
Usage: pub_sub_perf [options]

options:
    -n COUNT                         Messages to send (default: 100000}
    -s SIZE                          Message size (default: 16)
    -S SUBJECT                       Send subject (default: (test)
    -b BATCH                         Batch size (default: (100)
    """
    print(message)

def show_usage_and_die():
    show_usage()
    sys.exit(1)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--count', default=DEFAULT_NUM_MSGS, type=int)
    parser.add_argument('-s', '--size', default=DEFAULT_MSG_SIZE, type=int)
    parser.add_argument('-S', '--subject', default='test')
    parser.add_argument('-b', '--batch', default=DEFAULT_BATCH_SIZE, type=int)
    parser.add_argument('--servers', default=[], action='append')
    args = parser.parse_args()

    data = []
    for i in range(0, args.size):
        s = "%01x" % randint(0, 15)
        data.append(s.encode())
    payload = b''.join(data)

    servers = args.servers
    if len(args.servers) < 1:
        servers = ["nats://127.0.0.1:4222"]

    # Make sure we're connected to a server first...
    try:
        nc = await nats.connect(servers)
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}")
        show_usage_and_die()

    received = 0
    async def handler(msg):
        nonlocal received
        received += 1
        if (received % HASH_MODULO) == 0:
            sys.stdout.write("*")
            sys.stdout.flush()
    await nc.subscribe(args.subject, cb=handler)

    # Start the benchmark
    start = time.time()
    to_send = args.count

    print("Sending {} messages of size {} bytes on [{}]".format(
        args.count, args.size, args.subject))
    while to_send > 0:
        for i in range(0, args.batch):
            to_send -= 1
            await nc.publish(args.subject, payload)
            if (to_send % HASH_MODULO) == 0:
                sys.stdout.write("#")
                sys.stdout.flush()
            if to_send == 0:
                break

        # Minimal pause in between batches of commands sent to server
        await asyncio.sleep(0.00001)

    # Additional roundtrip with server to ensure everything has been
    # processed by the server already.
    try:
        while received < args.count:
            await nc.flush(DEFAULT_FLUSH_TIMEOUT)
    except nats.aio.errors.ErrTimeout:
        print(f"Server flush timeout after {DEFAULT_FLUSH_TIMEOUT}")

    elapsed = time.time() - start
    mbytes = "%.1f" % (((args.size * args.count)/elapsed) / (1024*1024))
    print("\nTest completed : {} msgs/sec sent ({}) MB/sec".format(
        args.count/elapsed,
        mbytes))

    print("Received {} messages ({} msgs/sec)".format(received, received/elapsed))
    await nc.close()

if __name__ == '__main__':
  asyncio.run(main())
