import asyncio

from nats import client


# NATS-DOC-START
async def main():
    # Connect to NATS demo server
    nc = await client.connect("nats://demo.nats.io")

    # Publish a message to the subject "hello"
    await nc.publish("hello", b"Hello NATS!")
    await nc.flush()

    print("Message published to hello")

    await nc.close()
# NATS-DOC-END


if __name__ == "__main__":
    asyncio.run(main())
