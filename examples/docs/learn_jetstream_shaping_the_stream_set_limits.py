import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Cap ORDERS with a seven-day age limit and a 1 GiB byte ceiling. Fetch the
    # current config, set the limits, and update the stream in place; the stored
    # messages stay put. max_age is in seconds.
    info = await js.stream_info("ORDERS")
    config = info.config
    config.max_age = 7 * 24 * 3600  # 7 days
    config.max_bytes = 1024 * 1024 * 1024  # 1 GiB
    await js.update_stream(config)
    print("ORDERS capped at 7d age and 1 GiB")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
