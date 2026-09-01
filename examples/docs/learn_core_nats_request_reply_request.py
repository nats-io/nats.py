import asyncio

from nats.client import connect
from nats.client.errors import NoRespondersError


async def main():
    client = await connect("nats://localhost:4222")

    # NATS-DOC-START
    # Ask the inventory service whether an order's item is in stock. The client
    # creates a private inbox, sends the request, and waits up to the timeout
    # for one reply. A missing service surfaces immediately as
    # NoRespondersError; a slow one surfaces as TimeoutError.
    order = '{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}'
    try:
        reply = await client.request("orders.inventory.check", order.encode(), timeout=2.0)
        print(f"inventory replied: {reply.data.decode()}")
    except NoRespondersError:
        print("no inventory service is running")
    except TimeoutError:
        print("inventory service did not answer in time")
    # NATS-DOC-END

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
