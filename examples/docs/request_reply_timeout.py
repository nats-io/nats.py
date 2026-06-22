import asyncio

from nats import client
from nats.client.errors import NoRespondersError


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Request with custom timeout
    try:
        m = await nc.request("service", b"data", timeout=2)
        print(f"Response: {m.data.decode()}")
    except TimeoutError:
        print("Request timed out")
    except NoRespondersError:
        print("No responders for the request")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
