import asyncio

from nats import client


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Subscribe to 'weather.updates' synchronously
    sub = await nc.subscribe("weather.updates")

    # Process messages
    while True:
        try:
            msg = await sub.next(timeout=1)
            print(f"Received: {msg.data.decode()}")
        except TimeoutError:
            break
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
