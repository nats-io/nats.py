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

    # nats.py drives a pull consumer with fetch, so a continuous flow is a fetch
    # loop: pull a batch, process it, come back. fetch times out when nothing is
    # waiting; keep looping so new orders are picked up as soon as they land.
    while True:
        try:
            msgs = await psub.fetch(batch=10, timeout=5)
        except nats.errors.TimeoutError:
            continue
        for msg in msgs:
            print(f"shipping {msg.data.decode()}")
            await msg.ack()
    # NATS-DOC-END


if __name__ == "__main__":
    asyncio.run(main())
