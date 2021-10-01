import asyncio
from nats import NATS, Msg


async def run():
    nc = NATS()

    async def disconnected_cb() -> None:
        print("Got disconnected...")

    async def reconnected_cb() -> None:
        print("Got reconnected...")

    await nc.connect(
        "127.0.0.1",
        reconnected_cb=reconnected_cb,
        disconnected_cb=disconnected_cb,
        max_reconnect_attempts=-1,
    )

    async def help_request(msg: Msg) -> None:
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        headers = msg.headers or {}
        if (int(headers.get("idx", 0)) % 1000) == 0:
            print(
                "Received a message on '{subject} {reply} (headers: {headers}': {data}"
                .format(
                    subject=subject, headers=headers, reply=reply, data=data
                )
            )
        await nc.publish(reply, b'I can help')

    # Use queue named 'workers' for distributing requests
    # among subscribers.
    await nc.subscribe("help", "workers", help_request)

    print("Listening for requests on 'help' subject...")
    for i in range(1, 100000):
        try:
            response = await nc.request("help", b'hi', headers={"idx": str(i)})
            if (i % 1000) == 0:
                print(response)
        except Exception as e:
            print("Error:", e)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    try:
        loop.run_forever()
    finally:
        loop.close()
