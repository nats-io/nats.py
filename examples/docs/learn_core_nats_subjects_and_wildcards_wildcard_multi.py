import asyncio

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # Audit service: catch every order message at any depth. The multi-token
    # wildcard > matches one or more tokens and must be the last token, so
    # orders.> matches orders.created, orders.us.created, and
    # orders.us.west.created alike.
    async with await client.subscribe("orders.>") as subscription:
        async for message in subscription:
            print(f"audit: {message.subject}")
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
