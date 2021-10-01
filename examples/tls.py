import asyncio
import ssl
from nats import NATS, Msg
from nats.aio.errors import ErrTimeout


async def main():
    nc = NATS()

    # Your local server needs to configured with the server certificate in the ../tests/certs directory.
    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    ssl_ctx.minimum_version = ssl.PROTOCOL_TLSv1_2
    ssl_ctx.load_verify_locations('../tests/certs/ca.pem')
    ssl_ctx.load_cert_chain(
        certfile='../tests/certs/client-cert.pem',
        keyfile='../tests/certs/client-key.pem'
    )
    await nc.connect(servers=["nats://127.0.0.1:4222"], tls=ssl_ctx)

    async def message_handler(msg: Msg):
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
    await nc.subscribe("help", "workers", help_request)

    # Send a request and expect a single response
    # and trigger timeout if not faster than 50 ms.
    try:
        response = await nc.request("help", b'help me', 0.050)
        print(
            "Received response: {message}".format(
                message=response.data.decode()
            )
        )
    except ErrTimeout:
        print("Request timed out")

    await nc.close()


if __name__ == '__main__':
    asyncio.run(main())
