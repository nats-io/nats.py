import asyncio

from nats import client


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    async def worker(sub, name):
        async for msg in sub:
            print(f"Worker {name} Received: {msg.data.decode()}")

    sub_a = await nc.subscribe("orders.new", queue="new-orders-queue")
    sub_b = await nc.subscribe("orders.new", queue="new-orders-queue")
    sub_c = await nc.subscribe("orders.new", queue="new-orders-queue")

    asyncio.create_task(worker(sub_a, "A"))
    asyncio.create_task(worker(sub_b, "B"))
    asyncio.create_task(worker(sub_c, "C"))

    # flush() waits for the server to acknowledge all pending commands.
    # This ensures all subscriptions are at the server before publish starts.
    await nc.flush(timeout=1)

    for i in range(1, 11):
        await nc.publish("orders.new", f"Order {i}".encode())
    # NATS-DOC-END

    # give time for all the messages to be received
    await asyncio.sleep(1)
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
