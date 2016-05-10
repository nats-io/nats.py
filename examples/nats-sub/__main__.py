import argparse, sys
import asyncio
import os
import signal
from nats.aio.client import Client as NATS

def show_usage():
    usage = """
nats-sub SUBJECT [-s SERVER] [-q QUEUE]

Example:

nats-sub help -q workers -s nats://127.0.0.1:4222 -s nats://127.0.0.1:4223
"""
    print(usage)

def show_usage_and_die():
    show_usage()
    sys.exit(1)

def run(loop):
    parser = argparse.ArgumentParser()

    # e.g. nats-sub hello -s nats://127.0.0.1:4222
    parser.add_argument('subject', default='hello', nargs='?')
    parser.add_argument('-s', '--servers', default=[], action='append')
    parser.add_argument('-q', '--queue', default="")
    args = parser.parse_args()

    nc = NATS()

    @asyncio.coroutine
    def closed_cb():
        print("Connection to NATS is closed.")
        yield from asyncio.sleep(0.1, loop=loop)
        loop.stop()

    @asyncio.coroutine
    def reconnected_cb():
        print("Connected to NATS at {}...".format(nc.connected_url.netloc))

    @asyncio.coroutine
    def subscribe_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
          subject=subject, reply=reply, data=data))

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
    def signal_handler():
        if nc.is_closed:
            return
        print("Disconnecting...")
        loop.create_task(nc.close())

    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)

    yield from nc.subscribe(args.subject, args.queue, subscribe_handler)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
