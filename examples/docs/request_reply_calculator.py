import asyncio

from nats import client
from nats.client.errors import NoRespondersError


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Calculator service
    sub = await nc.subscribe("calc.add")

    async def calc_service():
        async for msg in sub:
            # data is in the form "x y"
            try:
                parts = msg.data.decode().split()
                if len(parts) == 2 and msg.reply:
                    x = int(parts[0])
                    y = int(parts[1])
                    await nc.publish(msg.reply, str(x + y).encode())
            except Exception:
                # you could send some other reply here
                pass

    asyncio.create_task(calc_service())
    await nc.flush()

    # Make a request with a timeout
    try:
        m = await nc.request("calc.add", b"5 3", timeout=0.5)
        print(f"5 + 3 = {m.data.decode()}")
    except (TimeoutError, NoRespondersError):
        print("1) No Response")

    # Make another request
    try:
        m = await nc.request("calc.add", b"10 7", timeout=0.5)
        print(f"10 + 7 = {m.data.decode()}")
    except (TimeoutError, NoRespondersError):
        print("2) No Response")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
