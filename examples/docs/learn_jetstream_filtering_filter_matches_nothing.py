import asyncio

import nats
import nats.errors
from nats.js.api import AckPolicy, ConsumerConfig


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Note the typo: "orders.shiped" matches no subject in the stream.
    # JetStream accepts the consumer anyway. The wrong filter fails silently.
    await js.add_consumer(
        "ORDERS",
        ConsumerConfig(
            durable_name="analytics-typo",
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject="orders.shiped",
        ),
    )

    psub = await js.pull_subscribe_bind("analytics-typo", stream="ORDERS")
    try:
        await psub.fetch(batch=5, timeout=2)
    except nats.errors.TimeoutError:
        # No error from the server. The pull simply returned nothing because the
        # filter matched no stored subject. A wrong filter looks like an empty stream.
        print("Fetch timed out: the filter matched no stored subject")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
