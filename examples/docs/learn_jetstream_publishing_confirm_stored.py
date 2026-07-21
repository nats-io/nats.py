import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Publish one order. The publish only resolves once the server has stored
    # the message, so reading the acknowledgement confirms it is durable.
    ack = await js.publish(
        "orders.created",
        b'{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}',
    )

    # The stream name and sequence number prove the message is stored.
    print(f"Confirmed stored in {ack.stream} at sequence {ack.seq}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
