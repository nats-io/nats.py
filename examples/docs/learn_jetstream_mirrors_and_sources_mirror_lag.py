import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # A mirror is eventually consistent. Read its lag before trusting it to
    # hold what the upstream just received: 0 means fully caught up.
    info = await js.stream_info("ORDERS-ARCHIVE")

    print(f"Upstream:  {info.mirror.name}")
    print(f"Lag:       {info.mirror.lag}")
    print(f"Last seen: {info.mirror.active}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
