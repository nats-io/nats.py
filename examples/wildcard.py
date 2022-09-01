import asyncio

from nats.aio.client import Client as NATS
from nats.errors import ConnectionClosedError, NoServersError, TimeoutError

from common import args


async def run(loop):
    nc = NATS()

    arguments, _ = args.get_args("Run the wildcard example.",
                                 "Usage: python examples/wildcard.py")
    await nc.connect(arguments.servers)

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    # "*" matches any token, at any level of the subject.
    await nc.subscribe("foo.*.baz", cb=message_handler)
    await nc.subscribe("foo.bar.*", cb=message_handler)

    # ">" matches any length of the tail of a subject, and can only be the last token
    # E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
    await nc.subscribe("foo.>", cb=message_handler)

    # Matches all of the above.
    await nc.publish("foo.bar.baz", b'Hello World')

    # Gracefully close the connection.
    await nc.drain()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
