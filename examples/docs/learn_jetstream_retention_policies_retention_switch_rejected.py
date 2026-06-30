import asyncio

import nats
from nats.js.api import RetentionPolicy, StreamConfig
from nats.js.errors import APIError


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams and consumers.
    js = nc.jetstream()

    # Make sure the FULFILLMENT WorkQueue stream exists.
    await js.add_stream(
        StreamConfig(
            name="FULFILLMENT",
            subjects=["fulfill.>"],
            retention=RetentionPolicy.WORK_QUEUE,
        )
    )

    # NATS-DOC-START
    # Read the current config and try to switch FULFILLMENT from WorkQueue to
    # Limits retention. The server refuses: retention is fixed at create time,
    # so you cannot turn a queue into a record (or back) on an existing stream.
    info = await js.stream_info("FULFILLMENT")
    try:
        await js.update_stream(info.config, retention=RetentionPolicy.LIMITS)
        print("retention changed (unexpected)")
    except APIError as e:
        print(f"retention switch rejected: {e}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
