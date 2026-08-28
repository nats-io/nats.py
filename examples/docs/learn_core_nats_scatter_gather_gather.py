import asyncio
import uuid

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # Scatter one request to every shipping-quote provider and gather the
    # replies. Subscribe to a private inbox, publish the request with that inbox
    # as the reply subject, then collect quotes until they stop arriving and
    # pick the cheapest.
    order = '{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}'
    inbox = f"_INBOX.{uuid.uuid4().hex}"
    async with await client.subscribe(inbox) as subscription:
        await client.publish("shipping.quote", order.encode(), reply=inbox)

        quotes = []
        while True:
            try:
                message = await subscription.next(timeout=0.3)
                quotes.append(message.data.decode())
            except TimeoutError:
                break

    print(f"gathered {len(quotes)} quotes: {quotes}")
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
