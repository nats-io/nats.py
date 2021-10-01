import asyncio
import nats
from nats.aio.errors import ErrTimeout, ErrNoServers


async def main():
    try:
        # Setting explicit list of servers in a cluster.
        nc = await nats.connect(
            servers=[
                "nats://127.0.0.1:4222", "nats://127.0.0.1:4223",
                "nats://127.0.0.1:4224"
            ]
        )
    except ErrNoServers as e:
        print(e)
        return

    async def message_handler(msg: nats.Msg) -> None:
        print("Request :", msg)
        await nc.publish(msg.reply, b"I can help!")

    await nc.subscribe("help.>", cb=message_handler)

    async def request_handler(msg: nats.Msg) -> None:
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print(
            "Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data
            )
        )

    # Signal the server to stop sending messages after we got 10 already.
    resp = await nc.request("help.please", b'help')
    print("Response:", resp)

    try:
        # Flush connection to server, returns when all messages have been processed.
        # It raises a timeout if roundtrip takes longer than 1 second.
        await nc.flush(1)
    except ErrTimeout:
        print("Flush timeout")

    await asyncio.sleep(1)

    # Drain gracefully closes the connection, allowing all subscribers to
    # handle any pending messages inflight that the server may have sent.
    await nc.drain()


if __name__ == '__main__':
    asyncio.run(main())
