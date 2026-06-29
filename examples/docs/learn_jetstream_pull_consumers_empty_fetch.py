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

    # On a drained consumer, fetch raises TimeoutError once the timeout passes.
    # Treat that as "nothing right now," not a failure: catch it, wait, and
    # fetch again instead of crashing.
    try:
        msgs = await psub.fetch(batch=10, timeout=2)
    except nats.errors.TimeoutError:
        print("no orders waiting, will retry")
        msgs = []

    for msg in msgs:
        print(f"shipping {msg.data.decode()}")
        await msg.ack()
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
