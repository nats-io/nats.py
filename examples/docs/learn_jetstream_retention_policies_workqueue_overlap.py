import asyncio

import nats
from nats.js.api import AckPolicy, ConsumerConfig, RetentionPolicy, StreamConfig
from nats.js.errors import APIError


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams and consumers.
    js = nc.jetstream()

    # Make sure the FULFILLMENT WorkQueue stream exists.
    await js.add_stream(
        StreamConfig(
            name="FULFILLMENT",
            subjects=["fulfill.>"],
            retention=RetentionPolicy.WORK_QUEUE,
        )
    )

    # NATS-DOC-START
    # One unfiltered consumer covers every fulfill.> subject. That is allowed.
    await js.add_consumer(
        "FULFILLMENT",
        ConsumerConfig(durable_name="shippers", ack_policy=AckPolicy.EXPLICIT),
    )
    print("added unfiltered consumer shippers")

    # A WorkQueue stream gives each message to exactly one consumer, so two
    # consumers whose subjects overlap would compete for the same orders. A
    # second unfiltered consumer overlaps with the first and is rejected.
    try:
        await js.add_consumer(
            "FULFILLMENT",
            ConsumerConfig(
                durable_name="eu-shippers", ack_policy=AckPolicy.EXPLICIT
            ),
        )
    except APIError as e:
        print(f"second unfiltered consumer rejected: {e}")

    # Drop the unfiltered consumer so the subject space is free again.
    await js.delete_consumer("FULFILLMENT", "shippers")

    # Filtered consumers work as long as their subjects do not overlap. Split
    # the queue by region: one worker pool per destination.
    await js.add_consumer(
        "FULFILLMENT",
        ConsumerConfig(
            durable_name="us-shippers",
            filter_subject="fulfill.us",
            ack_policy=AckPolicy.EXPLICIT,
        ),
    )
    await js.add_consumer(
        "FULFILLMENT",
        ConsumerConfig(
            durable_name="eu-shippers",
            filter_subject="fulfill.eu",
            ack_policy=AckPolicy.EXPLICIT,
        ),
    )
    print("added filtered consumers us-shippers and eu-shippers")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
