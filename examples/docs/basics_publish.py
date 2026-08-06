import asyncio

from nats import client


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Publish a message to the subject "weather.updates"
    await nc.publish("weather.updates", "Temperature: 72°F".encode())
    # NATS-DOC-END

    await nc.flush()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
