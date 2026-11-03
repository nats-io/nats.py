import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Add a per-subject ceiling so one noisy subject can't evict another's
    # messages. max_msgs_per_subject keeps the most recent N messages for every
    # subject independently, alongside the whole-stream limits.
    info = await js.stream_info("ORDERS")
    config = info.config
    config.max_msgs_per_subject = 100000
    await js.update_stream(config)
    print("ORDERS now keeps 100000 messages per subject")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
