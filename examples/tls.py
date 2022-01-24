import asyncio
import ssl

from nats.aio.client import Client as NATS
from nats.errors import TimeoutError


async def main():
    nc = NATS()

    # Your local server needs to configured with the server certificate in the ../tests/certs directory.
    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    ssl_ctx.minimum_version = ssl.PROTOCOL_TLSv1_2
    ssl_ctx.load_verify_locations('../tests/certs/ca.pem')
    ssl_ctx.load_cert_chain(
        certfile='../tests/certs/client-cert.pem',
        keyfile='../tests/certs/client-key.pem')
    await nc.connect(servers=["nats://127.0.0.1:4222"], tls=ssl_ctx)

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
    await nc.subscribe("help", "workers", help_request)

    # Send a request and expect a single response
    # and trigger timeout if not faster than 50 ms.
    try:
        response = await nc.timed_request("help", b'help me', 0.050)
        print("Received response: {message}".format(
            message=response.data.decode()))
    except TimeoutError:
        print("Request timed out")

    await asyncio.sleep(1)
    await nc.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
