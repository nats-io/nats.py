import asyncio

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Create a durable consumer that reads the whole stream from the start.
    # add_consumer is idempotent: calling it again with the same config is a no-op.
    await js.add_consumer(
        "ORDERS",
        ConsumerConfig(
            durable_name="billing",
            ack_policy=AckPolicy.EXPLICIT,
            deliver_policy=DeliverPolicy.ALL,
        ),
    )
    print("Created durable consumer billing on stream ORDERS")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
