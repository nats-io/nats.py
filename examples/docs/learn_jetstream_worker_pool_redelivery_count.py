import asyncio

import nats
import nats.errors


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Bind to the durable "shipping" consumer.
    psub = await js.pull_subscribe_bind("shipping", stream="ORDERS")

    # Each message records how many times it has been delivered. A count above
    # one means a redelivery: the server handed this order out before, but a
    # worker crashed or ran past AckWait before acking. Key your side effects by
    # order_id so handling the same order twice is harmless.
    try:
        msgs = await psub.fetch(batch=10, timeout=1)
    except nats.errors.TimeoutError:
        msgs = []

    for msg in msgs:
        if msg.metadata.num_delivered > 1:
            print(f"redelivery #{msg.metadata.num_delivered} of {msg.data.decode()}")
        else:
            print(f"first delivery of {msg.data.decode()}")
        await msg.ack()
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
