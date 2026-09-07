import asyncio

from nats import client, jetstream


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await client.connect("nats://127.0.0.1:4222")

    # NATS-DOC-START
    # JetStream context
    js = jetstream.new(nc)

    # Create a stream that captures any subject under `orders.`
    stream = await js.create_stream(name="ORDERS", subjects=["orders.>"], storage="file")

    # Publish a few orders
    await js.publish("orders.new", b"Order #1001")
    await js.publish("orders.new", b"Order #1002")
    await js.publish("orders.shipped", b"Order #1001 shipped")

    # Create a durable pull consumer that delivers from the beginning
    consumer = await stream.create_or_update_consumer(
        name="order-processor",
        ack_policy="explicit",
    )

    # Fetch a batch and acknowledge each message
    batch = await consumer.fetch(max_messages=3, max_wait=2.0)
    async for msg in batch:
        print(f"Received on {msg.subject}: {msg.data.decode()}")
        await msg.ack()
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
