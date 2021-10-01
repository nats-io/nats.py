import asyncio
from nats import NATS, Msg
from nats.aio.errors import ErrTimeout


async def main():
    nc = NATS()

    # It is very likely that the demo server will see traffic from clients other than yours.
    # To avoid this, start your own locally and modify the example to use it.
    # await nc.connect("nats://127.0.0.1:4222")
    await nc.connect("nats://demo.nats.io:4222")

    async def message_handler(msg: Msg) -> None:
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print(
            "Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data
            )
        )

    # Simple publisher and async subscriber via coroutine. Stop receiving after 2 messages.
    sub = await nc.subscribe("foo", cb=message_handler, max_msgs=2)

    await nc.publish("foo", b'Hello')
    await nc.publish("foo", b'World')
    # Will not be received
    await nc.publish("foo", b'!!!!!')

    async def help_request(msg: Msg) -> None:
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print(
            "Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data
            )
        )
        await nc.publish(reply, b'I can help')

    # Use queue named 'workers' for distributing requests
    # among subscribers.
    sub = await nc.subscribe("help", "workers", help_request)

    # Drain the subscription
    await sub.drain()

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
            print(
                "Received response: {message}".format(
                    message=response.data.decode()
                )
            )
    except ErrTimeout:
        pass

    await sub.drain()
    # Terminate connection to NATS.
    await nc.close()


if __name__ == '__main__':
    asyncio.run(main())
