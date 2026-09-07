import asyncio
import uuid

from nats.client import connect


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # Gather more than one reply to a single request. A plain request() returns
    # only the first reply, so when several services may answer, subscribe to
    # your own inbox and collect replies until they stop arriving.
    order = '{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}'
    inbox = f"_INBOX.{uuid.uuid4().hex}"
    async with await client.subscribe(inbox) as subscription:
        await client.publish("orders.inventory.check", order.encode(), reply=inbox)

        replies = []
        while True:
            try:
                # Stop once no further reply arrives within the gap deadline.
                message = await subscription.next(timeout=0.3)
                replies.append(message.data.decode())
            except TimeoutError:
                break

    print(f"gathered {len(replies)} replies: {replies}")
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
