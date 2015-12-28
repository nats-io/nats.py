import asyncio
from nats.io.client import Client as NATS
from nats.io.errors import ErrConnectionClosed, ErrTimeout

def go(loop):
    nc = NATS()

    try:
        yield from nc.connect(io_loop=loop)
    except:
        pass

    def message_handler(msg):
        print("[Received on '{}']: {}".format(msg.subject.decode(), msg.data.decode()))

    if nc.is_connected:
        yield from nc.subscribe("discover", "", message_handler)

        try:
            yield from nc.publish("discover", b'hello world')

            # Make a roundtrip to the server.
            yield from nc.flush(0.500)
        except ErrConnectionClosed:
            print("Connection closed prematurely")
            return
        except ErrTimeout:
            print("Flush Timeout!")

        # Wait a bit for message to be dispatched...
        yield from asyncio.sleep(1, loop=loop)

        # Detach from the server.
        yield from nc.close()

        try:
            yield from nc.publish("discover", b"hello again!")
        except ErrConnectionClosed:
            print("Can't publish since connection is already closed.")

    if nc.last_error is not None:
        print("Last Error: {}".format(nc.last_error))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(go(loop))
    loop.close()
