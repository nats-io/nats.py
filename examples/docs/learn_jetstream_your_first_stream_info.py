import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Fetch the latest information about the "ORDERS" stream.
    info = await js.stream_info("ORDERS")

    # Print key fields: name and subjects come from the config,
    # the live message count comes from the stream state.
    print(f"Stream:   {info.config.name}")
    print(f"Subjects: {info.config.subjects}")
    print(f"Messages: {info.state.messages}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
