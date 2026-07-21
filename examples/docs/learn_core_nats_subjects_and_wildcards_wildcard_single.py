import asyncio

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # Regional analytics: one subscription catches created orders from every
    # region. The single-token wildcard * matches exactly one token, so both
    # orders.us.created and orders.eu.created match, while orders.created and
    # orders.us.west.created do not.
    async with await client.subscribe("orders.*.created") as subscription:
        async for message in subscription:
            print(f"analytics: new order on {message.subject}")
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
