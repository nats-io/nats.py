import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Get one stored message by its sequence number — the number the PubAck
    # returned when it was published. This regular get is served by the
    # stream's leader, so it always sees the latest write.
    msg = await js.get_msg("ORDERS", seq=2)

    print(f"seq {msg.seq} on {msg.subject}: {msg.data.decode()}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
