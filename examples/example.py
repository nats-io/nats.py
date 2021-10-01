import asyncio
from nats import NATS, Msg
from nats.aio.errors import ErrConnectionClosed, ErrTimeout


async def go():
    nc = NATS()

    try:
        # It is very likely that the demo server will see traffic from clients other than yours.
        # To avoid this, start your own locally and modify the example to use it.
        # await nc.connect(servers=["nats://127.0.0.1:4222"])
        await nc.connect(servers=["nats://demo.nats.io:4222"])
    except:
        pass

    async def message_handler(msg: Msg) -> None:
        print(f"[Received on '{msg.subject}']: {msg.data.decode()}")

    try:
        # Interested in receiving 2 messages from the 'discover' subject.
        sub = await nc.subscribe("discover", "", message_handler, max_msgs=2)

        await nc.publish("discover", b'hello')
        await nc.publish("discover", b'world')

        # Following 2 messages won't be received.
        await nc.publish("discover", b'again')
        await nc.publish("discover", b'!!!!!')
    except ErrConnectionClosed:
        print("Connection closed prematurely")

    async def request_handler(msg: Msg) -> None:
        print(
            "[Request on '{} {}']: {}".format(
                msg.subject, msg.reply, msg.data.decode()
            )
        )
        await nc.publish(msg.reply, b'OK')

    if nc.is_connected:

        # Subscription using a 'workers' queue so that only a single subscriber
        # gets a request at a time.
        await nc.subscribe("help", "workers", cb=request_handler)

        try:
            # Make a request expecting a single response within 500 ms,
            # otherwise raising a timeout error.
            msg = await nc.request("help", b'help please', 0.500)
            print(f"[Response]: {msg.data}")

        except ErrTimeout:
            print("[Error] Timeout!")

        # Detach from the server.
        await nc.close()

    if nc.last_error is not None:
        print(f"Last Error: {nc.last_error}")

    if nc.is_closed:
        print("Disconnected.")


if __name__ == '__main__':
    asyncio.run(go())
