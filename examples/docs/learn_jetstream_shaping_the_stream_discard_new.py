import asyncio

import nats
from nats.js.api import DiscardPolicy
from nats.js.errors import APIError


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # NATS-DOC-START
    # Switch ORDERS to Discard New. Discard New never drops stored messages, so
    # capping it at one message leaves the existing orders in place and puts the
    # stream instantly over its limit; the next publish is rejected.
    info = await js.stream_info("ORDERS")
    config = info.config
    config.discard = DiscardPolicy.NEW
    config.max_msgs = 1
    await js.update_stream(config)

    # This publish hits the full stream and is rejected instead of succeeding
    # silently. Handle it in the publisher.
    try:
        await js.publish("orders.created", b'{"order_id":"ord_8w2k"}')
    except APIError as err:
        print(f"publish rejected: {err.description}")

    # Put ORDERS back: Discard Old, no message cap (age and byte limits stay).
    config.discard = DiscardPolicy.OLD
    config.max_msgs = -1
    await js.update_stream(config)
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
