import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Bind to the durable "shipping" consumer.
    psub = await js.pull_subscribe_bind("shipping", stream="ORDERS")

    # Fetch a batch of up to 10 orders, waiting up to 2 seconds for them. The
    # call returns the messages it has when the batch is full or the timeout
    # passes. Process and ack each, then fetch again to keep going.
    msgs = await psub.fetch(batch=10, timeout=2)
    for msg in msgs:
        print(f"shipping {msg.data.decode()}")
        await msg.ack()
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
