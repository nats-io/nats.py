import asyncio

from nats.aio.client import Client as NATS
from nats.errors import ConnectionClosedError, TimeoutError

from common import args


async def main():
    nc = NATS()

    try:
        arguments, _ = args.get_args("Run an example.")
        await nc.connect(servers=arguments.servers)
    except:
        pass

    async def message_handler(msg):
        print(f"[Received on '{msg.subject}']: {msg.data.decode()}")

    try:
        # Interested in receiving 2 messages from the 'discover' subject.
        sub = await nc.subscribe("discover", "", message_handler)
        await sub.unsubscribe(2)

        await nc.publish("discover", b'hello')
        await nc.publish("discover", b'world')

        # Following 2 messages won't be received.
        await nc.publish("discover", b'again')
        await nc.publish("discover", b'!!!!!')
    except ConnectionClosedError:
        print("Connection closed prematurely")

    async def request_handler(msg):
        print("[Request on '{} {}']: {}".format(msg.subject, msg.reply,
                                                msg.data.decode()))
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

            # Make a roundtrip to the server to ensure messages
            # that sent messages have been processed already.
            await nc.flush(0.500)
        except ErrTimeout:
            print("[Error] Timeout!")

        # Wait a bit for message to be dispatched...
        await asyncio.sleep(1)

        # Detach from the server.
        await nc.close()

    if nc.last_error is not None:
        print(f"Last Error: {nc.last_error}")

    if nc.is_closed:
        print("Disconnected.")


if __name__ == '__main__':
    asyncio.run(main())
