import asyncio

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Raise max_ack_pending so a larger pool can hold more orders in progress at
    # once. The cap is shared across the whole "shipping" consumer, not per
    # worker, so size it to at least your worker count. add_consumer with an
    # existing durable updates it in place.
    await js.add_consumer(
        "ORDERS",
        ConsumerConfig(
            durable_name="shipping",
            ack_policy=AckPolicy.EXPLICIT,
            deliver_policy=DeliverPolicy.ALL,
            max_ack_pending=5000,
        ),
    )
    print("shipping max_ack_pending set to 5000")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
