import asyncio

from nats import client


# NATS-DOC-START
async def main():
    nc = await client.connect("nats://demo.nats.io")

    # Asynchronous subscriber - iterate messages in a background task
    async_sub = await nc.subscribe("hello")

    async def async_handler():
        async for msg in async_sub:
            print(f"Asynchronous Subscriber Received: {msg.data.decode()}")

    asyncio.create_task(async_handler())

    # Synchronous subscription
    sync_sub = await nc.subscribe("hello")

    print("Waiting for message on 'hello'")

    # Process messages synchronously
    while True:
        try:
            msg = await sync_sub.next(timeout=1)
            print(f"Synchronous Subscriber Received: {msg.data.decode()}")
        except TimeoutError:
            break

    await nc.close()
# NATS-DOC-END


if __name__ == "__main__":
    asyncio.run(main())
