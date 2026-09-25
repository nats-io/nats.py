import asyncio

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # Publish one order to the orders.created subject. Publishing is
    # fire-and-forget: the call hands the message to the server and returns.
    order = '{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}'
    await client.publish("orders.created", order.encode())
    # NATS-DOC-END

    await client.flush()
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
