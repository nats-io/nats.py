import asyncio

import nats
from nats.js.api import StreamConfig, SubjectTransform


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Create a stream that rewrites subjects as it stores them. Incoming
    # ingest.<customer> messages become orders.<bucket>.<customer>, where
    # partition(3,1) hashes the first wildcard token into one of three
    # buckets — sharding each customer into a fixed partition.
    info = await js.add_stream(
        StreamConfig(
            name="ORDERS-SHARDED",
            subjects=["ingest.*"],
            subject_transform=SubjectTransform(
                src="ingest.*",
                dest="orders.{{partition(3,1)}}.{{wildcard(1)}}",
            ),
        )
    )

    print(f"created stream {info.config.name}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
