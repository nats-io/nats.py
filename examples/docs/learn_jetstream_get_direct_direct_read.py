import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Read directly from the stream's store with the Direct Get API. With
    # direct=True the read is served by any server holding a copy of the
    # stream, not just the leader (the stream must have allow_direct set).
    msg = await js.get_msg("ORDERS", seq=1, direct=True)

    print(f"seq {msg.seq} on {msg.subject} (direct): {msg.data.decode()}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
