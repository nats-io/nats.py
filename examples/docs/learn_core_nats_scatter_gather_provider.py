import asyncio

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # A shipping-quote provider. Subscribe plainly to shipping.quote (NOT in a
    # queue group, so every provider sees each request) and reply with a price.
    # Run several copies, each quoting a different number.
    async with await client.subscribe("shipping.quote") as subscription:
        async for message in subscription:
            if message.reply:
                await client.publish(message.reply, b'{"carrier":"carrier-a","quote_cents":1500}')
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
