import argparse, sys
import asyncio
import os
import signal
from nats.aio.client import Client as NATS

def show_usage():
    usage = """
nats-pub SUBJECT [-d DATA] [-s SERVER]

Example:

nats-pub hello -d world -s nats://127.0.0.1:4222 -s nats://127.0.0.1:4223
"""
    print(usage)

def show_usage_and_die():
    show_usage()
    sys.exit(1)

def run(loop):
    parser = argparse.ArgumentParser()

    # e.g. nats-pub hello -d "world" -s nats://127.0.0.1:4222 -s nats://127.0.0.1:4223
    parser.add_argument('subject', default='hello', nargs='?')
    parser.add_argument('-d', '--data', default="hello world")
    parser.add_argument('-s', '--servers', default=[], action='append')
    args = parser.parse_args()

    nc = NATS()

    @asyncio.coroutine
    def closed_cb():
        print("Connection to NATS is closed.")

    @asyncio.coroutine
    def reconnected_cb():
        print("Connected to NATS at {}...".format(nc.connected_url.netloc))

    options = {
        "io_loop": loop,
        "closed_cb": closed_cb,
        "reconnected_cb": reconnected_cb
    }

    try:
        if len(args.servers) > 0:
            options['servers'] = args.servers

        yield from nc.connect(**options)
    except Exception as e:
        print(e)
        show_usage_and_die()

    print("Connected to NATS at {}...".format(nc.connected_url.netloc))
    yield from nc.publish(args.subject, args.data.encode())
    yield from nc.flush()
    yield from nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run(loop))
    finally:
        loop.close()
