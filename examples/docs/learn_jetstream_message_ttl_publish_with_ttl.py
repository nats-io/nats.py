import asyncio

import nats
from nats.js.api import StreamConfig


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # Create a stream that allows per-message TTLs. Without allow_msg_ttl the
    # server ignores the Nats-TTL header. Requires NATS Server 2.11+.
    await js.add_stream(
        StreamConfig(
            name="ORDERS",
            subjects=["orders.>"],
            allow_msg_ttl=True,
        )
    )

    # NATS-DOC-START
    # Publish with a Nats-TTL header so the server deletes this message 60
    # seconds after it's stored, no matter what the stream would otherwise keep.
    ack = await js.publish(
        "orders.cancelled",
        b'{"id": "A-1001", "reason": "customer"}',
        headers={"Nats-TTL": "60s"},
    )

    print(f"stored in {ack.stream} at sequence {ack.seq}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
