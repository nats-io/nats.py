import asyncio

from nats import client


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Create a wire tap for monitoring - subscribe to everything
    sub = await nc.subscribe(">")
    received = asyncio.Event()
    seen = 0

    async def monitor():
        nonlocal seen
        async for msg in sub:
            seen += 1
            print(f"[MONITOR] {msg.subject} --> {msg.data.decode()}")
            if seen >= 3:
                received.set()

    asyncio.create_task(monitor())
    await nc.flush()

    # Publish a message to various subjects
    await nc.publish("hello", b"Hello NATS!")
    await nc.publish("event.new", b"click")
    await nc.publish("weather.north.fr", "Temperature: 11°C".encode())

    print("Waiting for messages...")
    await received.wait()
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
