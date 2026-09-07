import asyncio

from nats.jetstream.headers import (
    NATS_BATCH_COMMIT,
    NATS_BATCH_COMMIT_FINAL,
    NATS_BATCH_ID,
    NATS_BATCH_SEQUENCE,
)

import nats
from nats import jetstream


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # JetStream context over the connection (jetstream.new takes the client).
    js = jetstream.new(nc)

    # Ensure the "ORDERS" stream exists with atomic batch publishing enabled.
    try:
        config = jetstream.StreamConfig(name="ORDERS", subjects=["orders.>"], allow_atomic=True)
        await js.create_stream(config)
    except jetstream.StreamNameAlreadyInUseError:
        pass

    # NATS-DOC-START
    # Atomic batch (ADR-50): tag each line item with one batch id and a rising
    # sequence, then mark the last one committed. The server stores all three or
    # none, and sends a single ack -- on the commit -- for the whole batch.
    batch = "ord_8w2k-batch"
    await js.client.request(
        "orders.created", b'{"sku":"TEE"}', headers={NATS_BATCH_ID: batch, NATS_BATCH_SEQUENCE: "1"}
    )
    await js.client.publish(
        "orders.created", b'{"sku":"MUG"}', headers={NATS_BATCH_ID: batch, NATS_BATCH_SEQUENCE: "2"}
    )
    ack = await js.publish(
        "orders.created",
        b'{"sku":"CAP"}',
        headers={NATS_BATCH_ID: batch, NATS_BATCH_SEQUENCE: "3", NATS_BATCH_COMMIT: NATS_BATCH_COMMIT_FINAL},
    )
    print(f"committed batch {ack.batch_id}: {ack.batch_size} messages stored")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
