import asyncio

from nats import client
from nats.client.errors import NoRespondersError


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Multiple responders - only the first response is returned to the requester
    sub_a = await nc.subscribe("calc.add")
    sub_b = await nc.subscribe("calc.add")

    async def service(sub, label):
        async for msg in sub:
            if msg.reply:
                await nc.publish(msg.reply, f"calculated result from {label}".encode())

    asyncio.create_task(service(sub_a, "A"))
    asyncio.create_task(service(sub_b, "B"))
    await nc.flush()

    # Gets one response
    try:
        m = await nc.request("calc.add", b"", timeout=0.5)
        print(f"Got response: {m.data.decode()}")
    except (TimeoutError, NoRespondersError):
        print("No Response")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
