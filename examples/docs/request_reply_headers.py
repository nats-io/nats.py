import asyncio

from nats import client
from nats.client.errors import NoRespondersError


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Header aware service
    sub = await nc.subscribe("service")

    async def service():
        async for msg in sub:
            response_headers: dict[str, str | list[str]] = {}
            if msg.headers:
                request_id = msg.headers.get("X-Request-ID") or ""
                response_headers["X-Response-ID"] = request_id
                for key, values in msg.headers.items():
                    response_headers[key] = values
            if msg.reply:
                await nc.publish(msg.reply, msg.data, headers=response_headers)

    asyncio.create_task(service())
    await nc.flush()

    # Make a request with headers
    headers = {"X-Request-ID": "123", "X-Priority": "high"}
    try:
        m = await nc.request("service", b"data", timeout=0.5, headers=headers)
        data = m.data.decode() if m.data else ""
        print(f"Response: {data}")
        if m.headers:
            print(f"Response ID: {m.headers.get('X-Response-ID')}")
    except (TimeoutError, NoRespondersError):
        print("No Response")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
