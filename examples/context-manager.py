import asyncio
import nats


async def main():

    is_done = asyncio.Future()

    async def closed_cb():
        print("Connection to NATS is closed.")
        is_done.set_result(True)

    # It is very likely that the demo server will see traffic from clients other than yours.
    # To avoid this, start your own locally and modify the example to use it.
    opts = {
        # "servers": ["nats://127.0.0.1:4222"],
        "servers": ["nats://demo.nats.io:4222"],
        "closed_cb": closed_cb
    }

    async with (await nats.connect(**opts)) as nc:
        print(f"Connected to NATS at {nc.connected_url.netloc}...")

        async def subscribe_handler(msg: nats.Msg) -> None:
            subject = msg.subject
            reply = msg.reply
            data = msg.data.decode()
            headers = msg.headers
            print(
                "Received a message on '{subject} {reply} ({headers})': {data}"
                .format(
                    subject=subject, reply=reply, data=data, headers=headers
                )
            )

        sub = await nc.subscribe("discover", cb=subscribe_handler)

        for i in range(1, 10):
            await nc.publish(
                "discover", b"hello world", headers={"idx": str(i)}
            )
            await asyncio.sleep(0.1)

        await sub.drain()

    await asyncio.wait_for(is_done, 60.0)


if __name__ == '__main__':
    asyncio.run(main())
