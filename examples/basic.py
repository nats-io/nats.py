import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run(loop):
    nc = NATS()

    # It is very likely that the demo server will see traffic from clients other than yours.
    # To avoid this, start your own locally and modify the example to use it.
    # await nc.connect("nats://127.0.0.1:4222", loop=loop)
    await nc.connect("nats://demo.nats.io:4222", loop=loop)

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    # Simple publisher and async subscriber via coroutine.
    sid = await nc.subscribe("foo", cb=message_handler)

    # Stop receiving after 2 messages.
    await nc.auto_unsubscribe(sid, 2)
    await nc.publish("foo", b'Hello')
    await nc.publish("foo", b'World')
    await nc.publish("foo", b'!!!!!')

    async def help_request(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))
        await nc.publish(reply, b'I can help')

    # Use queue named 'workers' for distributing requests
    # among subscribers.
    sid = await nc.subscribe("help", "workers", help_request)

    # Send a request and expect a single response
    # and trigger timeout if not faster than 500 ms.
    try:
        response = await nc.request("help", b'help me', 0.5)
        print("Received response: {message}".format(
            message=response.data.decode()))
    except ErrTimeout:
        print("Request timed out")

    # Remove interest in subscription.
    await nc.unsubscribe(sid)

    # Terminate connection to NATS.
    await nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
