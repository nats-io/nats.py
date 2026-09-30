import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Set "Nats-Msg-Id" so the server can detect duplicates within the stream's
    # duplicate window.
    payload = b'{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}'
    headers = {"Nats-Msg-Id": "ord_8w2k-created"}

    # Publish the same message twice with the same id.
    first = await js.publish("orders.created", payload, headers=headers)
    print(f"First:  sequence {first.seq}, duplicate {first.duplicate}")

    second = await js.publish("orders.created", payload, headers=headers)
    print(f"Second: sequence {second.seq}, duplicate {second.duplicate}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
