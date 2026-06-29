import asyncio

import nats
import nats.errors


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Bind to the durable "shipping" consumer created earlier. Run this same
    # program in several processes: they all share the one consumer, and the
    # server splits the stored orders across them, one order to one worker.
    psub = await js.pull_subscribe_bind("shipping", stream="ORDERS")

    # Pull one order at a time and ack it. fetch times out when nothing is
    # waiting; keep looping so the worker stays ready for the next order.
    while True:
        try:
            msgs = await psub.fetch(batch=1, timeout=5)
        except nats.errors.TimeoutError:
            continue

        for msg in msgs:
            print(f"shipping {msg.data.decode()}")
            await msg.ack()
    # NATS-DOC-END


if __name__ == "__main__":
    asyncio.run(main())
