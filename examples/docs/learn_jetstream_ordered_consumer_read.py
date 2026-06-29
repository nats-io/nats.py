import asyncio

import nats
from nats.js.api import DeliverPolicy


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Ask for an ordered consumer with ordered_consumer=True. There's no ack to
    # send: nats.py runs the consumer for you and recreates it if it ever misses
    # a message, so you read every order in stream order. deliver_policy=ALL
    # starts from the first order.
    psub = await js.subscribe(
        "orders.>",
        stream="ORDERS",
        ordered_consumer=True,
        deliver_policy=DeliverPolicy.ALL,
    )

    # Read the whole log once, in order, stopping when caught up (num_pending 0).
    while True:
        msg = await psub.next_msg(timeout=5)
        print(f"order {msg.data.decode()}")
        if msg.metadata.num_pending == 0:
            break
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
