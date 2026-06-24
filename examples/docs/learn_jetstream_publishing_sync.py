import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Publish three orders. Each publish waits for the server's acknowledgement,
    # which carries the stream name and the assigned sequence number.
    ack = await js.publish(
        "orders.created",
        b'{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}',
    )
    print(f"Stored in {ack.stream}, sequence {ack.seq}")

    ack = await js.publish(
        "orders.created",
        b'{"order_id":"ord_2zr9","customer":"globex","total_cents":7800,"ts":"2026-05-22T10:14:25Z"}',
    )
    print(f"Stored in {ack.stream}, sequence {ack.seq}")

    ack = await js.publish(
        "orders.shipped",
        b'{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:31Z"}',
    )
    print(f"Stored in {ack.stream}, sequence {ack.seq}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
