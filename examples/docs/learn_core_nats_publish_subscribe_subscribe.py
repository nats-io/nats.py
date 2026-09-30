import asyncio

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # Subscribe as the warehouse service to orders.created. Each matching
    # message is delivered to this subscription as it is published.
    async with await client.subscribe("orders.created") as subscription:
        async for message in subscription:
            print(f"warehouse received: {message.data.decode()}")
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
