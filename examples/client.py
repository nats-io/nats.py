import asyncio
from datetime import datetime
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout

class Client:

    def __init__(self, nc, loop=asyncio.get_event_loop()):
        self.nc = nc
        self.loop = loop

    @asyncio.coroutine
    def message_handler(self, msg):
        print("[Received on '{}']: {}".format(msg.subject, msg.data.decode()))

    @asyncio.coroutine
    def request_handler(self, msg):
        print("[Request on '{} {}']: {}".format(msg.subject, msg.reply, msg.data.decode()))
        yield from self.nc.publish(msg.reply, b"I can help!")

    def start(self):
        try:
            yield from self.nc.connect(io_loop=self.loop)
        except:
            pass

        nc = self.nc
        try:
            # Interested in receiving 2 messages from the 'discover' subject.
            sid = yield from nc.subscribe("discover", "", self.message_handler)
            yield from nc.auto_unsubscribe(sid, 2)

            yield from nc.publish("discover", b'hello')
            yield from nc.publish("discover", b'world')

            # Following 2 messages won't be received.
            yield from nc.publish("discover", b'again')
            yield from nc.publish("discover", b'!!!!!')
        except ErrConnectionClosed:
            print("Connection closed prematurely")

        if nc.is_connected:

            # Subscription using a 'workers' queue so that only a single subscriber
            # gets a request at a time.
            yield from nc.subscribe("help", "workers", self.request_handler)

            try:
                # Make a request expecting a single response within 500 ms,
                # otherwise raising a timeout error.
                start_time = datetime.now()
                response = yield from nc.timed_request("help", b'help please', 0.500)
                end_time = datetime.now()
                print("[Response]: {}".format(response.data))
                print("[Duration]: {}".format(end_time - start_time))

                # Make a roundtrip to the server to ensure messages
                # that sent messages have been processed already.
                yield from nc.flush(0.500)
            except ErrTimeout:
                print("[Error] Timeout!")

            # Wait a bit for messages to be dispatched...
            yield from asyncio.sleep(2, loop=self.loop)

            # Detach from the server.
            yield from nc.close()

        if nc.last_error is not None:
            print("Last Error: {}".format(nc.last_error))

        if nc.is_closed:
            print("Disconnected.")

if __name__ == '__main__':
    c = Client(NATS())
    c.loop.run_until_complete(c.start())
    c.loop.close()
