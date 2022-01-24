import asyncio

import nats


async def main():
    # Connect to NATS!
    nc = await nats.connect("localhost")

    # Receive messages on 'foo'
    sub = await nc.subscribe("foo")

    # Publish a message to 'foo'
    await nc.publish("foo", b'Hello from Python!')

    # Process a message
    msg = await sub.next_msg()
    print("Received:", msg)

    # Close NATS connection
    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
