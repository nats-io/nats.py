import asyncio
from nats import NATS, Msg


async def main():
    nc = NATS()

    # It is very likely that the demo server will see traffic from clients other than yours.
    # To avoid this, start your own locally and modify the example to use it.
    # await nc.connect("nats://127.0.0.1:4222")
    await nc.connect("nats://demo.nats.io:4222")

    async def message_handler(msg: Msg) -> None:
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print(
            "Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data
            )
        )

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
    asyncio.run(main())
