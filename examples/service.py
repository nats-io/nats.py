import asyncio
from nats.aio.client import Client as NATS

async def run(loop):
    nc = NATS()

    async def disconnected_cb():
        print("Got disconnected...")

    async def reconnected_cb():
        print("Got reconnected...")

    await nc.connect("127.0.0.1",
                     reconnected_cb=reconnected_cb,
                     disconnected_cb=disconnected_cb,
                     max_reconnect_attempts=-1,
                     loop=loop)

    async def help_request(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))
        await nc.publish(reply, b'I can help')

    # Use queue named 'workers' for distributing requests
    # among subscribers.
    await nc.subscribe("help", "workers", help_request)

    print("Listening for requests on 'help' subject...")
    for i in range(1, 1000000):
        await asyncio.sleep(1)
        try:
            response = await nc.request("help", b'hi')
            print(response)
        except Exception as e:
            print("Error:", e)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.run_forever()
    loop.close()
