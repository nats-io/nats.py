import asyncio

import nats
from nats.js.api import StreamSource


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # Setup: the three regional streams ALL-ORDERS aggregates, each with its own subjects.
    await js.add_stream(name="ORDERS-US", subjects=["us.orders.>"])
    await js.add_stream(name="ORDERS-EU", subjects=["eu.orders.>"])
    await js.add_stream(name="ORDERS-APAC", subjects=["apac.orders.>"])

    # NATS-DOC-START
    # Create ALL-ORDERS as an aggregate that sources the three regional streams
    # into one. Unlike a mirror, a stream can list several sources.
    info = await js.add_stream(
        name="ALL-ORDERS",
        sources=[
            StreamSource(name="ORDERS-US"),
            StreamSource(name="ORDERS-EU"),
            StreamSource(name="ORDERS-APAC"),
        ],
    )

    print(f"Created {info.config.name} sourcing {len(info.config.sources)} streams")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
