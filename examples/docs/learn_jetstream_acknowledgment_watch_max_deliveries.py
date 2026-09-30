import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # NATS-DOC-START
    # JetStream publishes an advisory when a message hits its max delivery limit.
    # Subscribe with the core API to watch them for the shipping consumer.
    subject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORDERS.shipping"

    async def handler(msg):
        print(f"max deliveries advisory: {msg.data.decode()}")

    await nc.subscribe(subject, cb=handler)
    print(f"Watching {subject}")

    # Keep the subscription open to receive advisories as they arrive.
    await asyncio.sleep(60)
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
