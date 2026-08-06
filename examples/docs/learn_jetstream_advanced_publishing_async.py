import asyncio

import nats


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # nats.py has no dedicated async-publish API: publish() already returns a
    # coroutine, so the async-publish pattern is to start them all and gather
    # the results instead of awaiting one at a time. The round trips overlap,
    # but the client does not bound how many are in flight -- check every ack,
    # and add your own limit for large bursts.
    orders = [
        b'{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200}',
        b'{"order_id":"ord_2zr9","customer":"globex","total_cents":7800}',
        b'{"order_id":"ord_5t1m","customer":"initech","total_cents":1500}',
        b'{"order_id":"ord_9p3x","customer":"hooli","total_cents":9900}',
    ]

    pending = [js.publish("orders.created", order) for order in orders]
    results = await asyncio.gather(*pending, return_exceptions=True)

    for i, result in enumerate(results, start=1):
        if isinstance(result, Exception):
            print(f"order {i} failed, re-publish it: {result}")
        else:
            print(f"order {i} stored at sequence {result.seq}")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
