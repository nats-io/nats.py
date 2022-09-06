import asyncio
from datetime import datetime

from nats.aio.client import Client as NATS
from nats.errors import ConnectionClosedError, TimeoutError

from common import args


class Client:
    def __init__(self, nc):
        self.nc = nc

    async def message_handler(self, msg):
        print(f"[Received on '{msg.subject}']: {msg.data.decode()}")

    async def request_handler(self, msg):
        print("[Request on '{} {}']: {}".format(msg.subject, msg.reply,
                                                msg.data.decode()))
        await self.nc.publish(msg.reply, b"I can help!")

    async def start(self):
        try:
            arguments, _ = args.get_args("Run a client example.")
            await self.nc.connect(servers=arguments.servers)
        except:
            pass

        nc = self.nc
        try:
            # Interested in receiving 2 messages from the 'discover' subject.
            sub = await nc.subscribe("discover", "", self.message_handler)
            await sub.unsubscribe(2)

            await nc.publish("discover", b'hello')
            await nc.publish("discover", b'world')

            # Following 2 messages won't be received.
            await nc.publish("discover", b'again')
            await nc.publish("discover", b'!!!!!')
        except ConnectionClosedError:
            print("Connection closed prematurely")

        if nc.is_connected:

            # Subscription using a 'workers' queue so that only a single subscriber
            # gets a request at a time.
            await nc.subscribe("help", "workers", self.request_handler)

            try:
                # Make a request expecting a single response within 500 ms,
                # otherwise raising a timeout error.
                start_time = datetime.now()
                response = await nc.request("help", b'help please', 0.500)
                end_time = datetime.now()
                print(f"[Response]: {response.data}")
                print("[Duration]: {}".format(end_time - start_time))

                # Make a roundtrip to the server to ensure messages
                # that sent messages have been processed already.
                await nc.flush(0.500)
            except TimeoutError:
                print("[Error] Timeout!")

            # Wait a bit for messages to be dispatched...
            await asyncio.sleep(2)

            # Detach from the server.
            await nc.close()

        if nc.last_error is not None:
            print(f"Last Error: {nc.last_error}")

        if nc.is_closed:
            print("Disconnected.")


if __name__ == '__main__':
    c = Client(NATS())
    asyncio.run(c.start())
