import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Publish one order and inspect the acknowledgement the server returns.
    ack = await js.publish(
        "orders.created",
        b'{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}',
    )

    # The PubAck reports which stream stored the message, the sequence it was
    # assigned, and whether it was rejected as a duplicate.
    print(f"Stream:    {ack.stream}")
    print(f"Sequence:  {ack.seq}")
    print(f"Duplicate: {ack.duplicate}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
