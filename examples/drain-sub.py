import asyncio

from nats.aio.client import Client as NATS
from nats.errors import ConnectionClosedError, NoServersError, TimeoutError

from common import args


async def main():
    nc = NATS()

    arguments, _ = args.get_args("Run the drain-sub example.")
    await nc.connect(arguments.servers)

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    # Simple publisher and async subscriber via coroutine.
    sub = await nc.subscribe("foo", cb=message_handler)

    # Stop receiving after 2 messages.
    await sub.unsubscribe(2)
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
    sub = await nc.subscribe("help", "workers", help_request)

    async def drain_sub():
        await asyncio.sleep(0.001)

        print("Start draining subscription...")
        drain_task = await nc.drain(sid=sid)
        try:
            await asyncio.wait_for(drain_task, 2)
        except asyncio.TimeoutError:
            print("Took too long to drain subscription!")

    asyncio.create_task(drain_sub())

    # Send multiple requests and drain the subscription.
    requests = []
    for i in range(0, 100):
        request = nc.request("help", b'help me', 0.5)
        requests.append(request)

    # Wait for all the responses
    try:
        responses = await asyncio.gather(*requests)

        print("Received {count} responses!".format(count=len(responses)))

        for response in responses[:5]:
            print("Received response: {message}".format(
                message=response.data.decode()))
    except:
        pass

    # Terminate connection to NATS.
    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
