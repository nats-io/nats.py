import asyncio

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # Join the "packers" queue group on orders.created. Every subscriber that
    # names the same group shares the load: each order is delivered to exactly
    # one member. Run this in several processes to watch the load balance.
    async with await client.subscribe("orders.created", queue="packers") as subscription:
        async for message in subscription:
            print(f"packer handling: {message.data.decode()}")
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
