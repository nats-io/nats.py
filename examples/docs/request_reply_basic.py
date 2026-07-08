import asyncio
from datetime import datetime, timezone

from nats import client
from nats.client.errors import NoRespondersError


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Set up a service
    sub = await nc.subscribe("time")

    async def time_service():
        async for msg in sub:
            now = datetime.now(timezone.utc).isoformat()
            if msg.reply:
                await nc.publish(msg.reply, now.encode())

    asyncio.create_task(time_service())
    await nc.flush()

    # Make a request with a timeout and direct response
    try:
        m = await nc.request("time", b"", timeout=1)
        print(f"Response: {m.data.decode()}")
    except (TimeoutError, NoRespondersError):
        print("Request failed, no response.")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
