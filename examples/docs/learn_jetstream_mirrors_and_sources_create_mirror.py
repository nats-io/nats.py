import asyncio

import nats
from nats.js.api import StreamSource


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Create ORDERS-ARCHIVE as a read-only mirror of ORDERS. A mirror takes
    # no subjects of its own; it follows the upstream stream.
    info = await js.add_stream(
        name="ORDERS-ARCHIVE", mirror=StreamSource(name="ORDERS")
    )

    print(f"Created mirror {info.config.name} of {info.config.mirror.name}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
