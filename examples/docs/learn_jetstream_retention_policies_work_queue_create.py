import asyncio
import json

import nats
from nats.js.api import AckPolicy, ConsumerConfig, RetentionPolicy, StreamConfig


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams and consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # FULFILLMENT is the queue of paid orders awaiting shipment. WorkQueue
    # retention delivers each order to a single worker; the first ack removes
    # it for everyone, so the stream drains to empty.
    info = await js.add_stream(
        StreamConfig(
            name="FULFILLMENT",
            subjects=["fulfill.>"],
            retention=RetentionPolicy.WORK_QUEUE,
        )
    )
    print(f"FULFILLMENT retention is {info.config.retention}")

    # A paid order arrives and needs shipping.
    await js.publish(
        "fulfill.us",
        json.dumps({"order_id": "ord_8w2k", "customer": "acme-co"}).encode(),
    )

    # Shipping workers pull from a durable consumer with explicit ack.
    await js.add_consumer(
        "FULFILLMENT",
        ConsumerConfig(durable_name="shippers", ack_policy=AckPolicy.EXPLICIT),
    )

    # A worker takes one order and acks it once the order has shipped.
    psub = await js.pull_subscribe_bind("shippers", stream="FULFILLMENT")
    msgs = await psub.fetch(batch=1, timeout=5)
    await msgs[0].ack()
    print(f"shipped {msgs[0].data.decode()}")

    # The ack removed the task, so the WorkQueue stream is now empty. A Limits
    # stream like ORDERS would still hold the message after the ack.
    info = await js.stream_info("FULFILLMENT")
    print(f"messages left in FULFILLMENT: {info.state.messages}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
