import asyncio

from nats import client
from nats.client.errors import NoRespondersError


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    try:
        m = await nc.request("no.such.service", b"test", timeout=0.5)
        print(f"Response: {m.data.decode()}")
    except NoRespondersError:
        print("No services available to handle request")
    except TimeoutError:
        print("Request timed out")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
