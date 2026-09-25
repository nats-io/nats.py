import asyncio

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # The inventory service: subscribe to orders.inventory.check and answer
    # every request by publishing back to the reply subject each one carries.
    async with await client.subscribe("orders.inventory.check") as subscription:
        async for message in subscription:
            if message.reply:
                await client.publish(message.reply, b'{"in_stock":true,"warehouse":"us-east"}')
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
