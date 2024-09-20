import asyncio

import nats
from nats.errors import NoServersError, TimeoutError


async def main():
    async def disconnected_cb():
        print('Got disconnected!')

    async def reconnected_cb():
        print(f'Got reconnected to {nc.connected_url.netloc}')

    async def error_cb(e):
        print(f'There was an error: {e}')

    async def closed_cb():
        print('Connection is closed')

    try:
        # Setting explicit list of servers in a cluster.
        nc = await nats.connect("localhost:4222",
                                error_cb=error_cb,
                                reconnected_cb=reconnected_cb,
                                disconnected_cb=disconnected_cb,
                                closed_cb=closed_cb,
                                )
    except NoServersError as e:
        print(e)
        return

    async def message_handler(msg):
        print("Request :", msg)
        await nc.publish(msg.reply, b"I can help!")

    await nc.subscribe("help.>", cb=message_handler)

    async def request_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    # Signal the server to stop sending messages after we got 10 already.
    resp = await nc.request("help.please", b'help')
    print("Response:", resp)

    try:
        # Flush connection to server, returns when all messages have been processed.
        # It raises a timeout if roundtrip takes longer than 1 second.
        await nc.flush(1)
    except TimeoutError:
        print("Flush timeout")

    await asyncio.sleep(1)

    # Drain gracefully closes the connection, allowing all subscribers to
    # handle any pending messages inflight that the server may have sent.
    await nc.drain()

if __name__ == '__main__':
    asyncio.run(main())
