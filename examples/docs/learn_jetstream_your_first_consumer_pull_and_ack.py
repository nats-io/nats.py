import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Bind to the durable consumer and pull a single message.
    psub = await js.pull_subscribe_bind("shipping", stream="ORDERS")

    msgs = await psub.fetch(batch=1, timeout=5)
    msg = msgs[0]
    print(f"{msg.subject}: {msg.data.decode()}")

    # Explicit ack removes the message from this consumer's pending list.
    await msg.ack()
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
