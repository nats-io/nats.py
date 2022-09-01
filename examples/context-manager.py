import asyncio

import nats

from common import args


async def main():

    is_done = asyncio.Future()

    async def closed_cb():
        print("Connection to NATS is closed.")
        is_done.set_result(True)

    arguments, _ = args.get_args("Run a context manager example.")
    async with (await nats.connect(arguments.servers, closed_cb=closed_cb)) as nc:
        print(f"Connected to NATS at {nc.connected_url.netloc}...")

        async def subscribe_handler(msg):
            subject = msg.subject
            reply = msg.reply
            data = msg.data.decode()
            print("Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data))

        await nc.subscribe("discover", cb=subscribe_handler)
        await nc.flush()

        sub = await nc.subscribe("discover")

        for i in range(0, 10):
            await nc.publish("discover", b"hello world")
            await asyncio.sleep(0.1)

        count = 0
        async for msg in sub.messages:
            print("Received:", msg)
            count += 1
            if count >= 10:
                break

    await asyncio.wait_for(is_done, 60.0)

if __name__ == '__main__':
    asyncio.run(main())
