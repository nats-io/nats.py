import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # Fetch the current ORDERS configuration so we can flip one field.
    info = await js.stream_info("ORDERS")
    config = info.config

    # NATS-DOC-START
    # Turn on Direct Get for the stream. With allow_direct enabled, get-message
    # requests can be served by any replica instead of only the stream leader.
    config.allow_direct = True
    updated = await js.update_stream(config)

    print(f"allow_direct is now {updated.config.allow_direct}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
