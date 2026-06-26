import asyncio

import nats
import nats.errors


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages consumers.
    js = nc.jetstream()

    # NATS-DOC-START
    # Bind to the durable consumer created earlier and pull every stored message.
    psub = await js.pull_subscribe_bind("billing", stream="ORDERS")

    # Fetch in batches until a fetch times out, which means the stream is drained.
    # The consumer uses explicit ack, so acknowledge each message after handling it.
    while True:
        try:
            msgs = await psub.fetch(batch=100, timeout=1)
        except nats.errors.TimeoutError:
            break

        for msg in msgs:
            print(
                f"stream seq {msg.metadata.sequence.stream}, "
                f"consumer seq {msg.metadata.sequence.consumer}: "
                f"{msg.data.decode()}"
            )
            await msg.ack()
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
