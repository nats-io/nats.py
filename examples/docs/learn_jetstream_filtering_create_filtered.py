import asyncio

import nats
from nats.js.api import AckPolicy, ConsumerConfig


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Create a durable pull consumer that only sees orders.shipped.
    # The filter is applied server-side: orders.created never reaches this consumer.
    await js.add_consumer(
        "ORDERS",
        ConsumerConfig(
            durable_name="analytics",
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject="orders.shipped",
        ),
    )
    print("Created filtered consumer analytics on stream ORDERS")

    # Fetch a small batch. Only orders.shipped messages come back.
    psub = await js.pull_subscribe_bind("analytics", stream="ORDERS")
    msgs = await psub.fetch(batch=5, timeout=2)
    for msg in msgs:
        print(f"got {msg.subject}")
        await msg.ack()
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
