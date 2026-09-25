import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Create the "ORDERS" stream, capturing every subject under "orders.".
    info = await js.add_stream(name="ORDERS", subjects=["orders.>"])

    # Confirm success by printing the stream name the server returned.
    print(f"Created stream: {info.config.name}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
