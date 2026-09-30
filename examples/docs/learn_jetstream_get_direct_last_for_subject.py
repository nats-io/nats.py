import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Get the most recent message stored on a subject, when you know the
    # subject but not the sequence. This is the read a key-value lookup uses.
    msg = await js.get_last_msg("ORDERS", "orders.shipped")

    print(f"seq {msg.seq} on {msg.subject}: {msg.data.decode()}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
