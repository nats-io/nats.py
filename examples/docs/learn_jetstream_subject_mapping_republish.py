import asyncio

import nats
from nats.js.api import RePublish


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # Fetch the current ORDERS configuration so we can add republish to it.
    info = await js.stream_info("ORDERS")
    config = info.config

    # NATS-DOC-START
    # Republish every stored orders.> message onto a dash.orders.> subject, so
    # core subscribers can watch the stream live without a consumer.
    # Set headers_only=True to republish just the headers, not the body.
    config.republish = RePublish(
        src="orders.>",
        dest="dash.orders.>",
        # headers_only=True,
    )
    updated = await js.update_stream(config)

    print(f"republishing to {updated.config.republish.dest}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
