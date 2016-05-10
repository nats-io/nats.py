import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout

def go(loop):
    nc = NATS()

    try:
        yield from nc.connect(io_loop=loop)
    except:
        pass

    @asyncio.coroutine
    def message_handler(msg):
        print("[Received on '{}']: {}".format(msg.subject, msg.data.decode()))

    try:
        # Interested in receiving 2 messages from the 'discover' subject.
        sid = yield from nc.subscribe("discover", "", message_handler)
        yield from nc.auto_unsubscribe(sid, 2)

        yield from nc.publish("discover", b'hello')
        yield from nc.publish("discover", b'world')

        # Following 2 messages won't be received.
        yield from nc.publish("discover", b'again')
        yield from nc.publish("discover", b'!!!!!')
    except ErrConnectionClosed:
        print("Connection closed prematurely")

    @asyncio.coroutine
    def request_handler(msg):
        print("[Request on '{} {}']: {}".format(msg.subject, msg.reply, msg.data.decode()))
        yield from nc.publish(msg.reply, b'OK')

    if nc.is_connected:
        
        # Subscription using a 'workers' queue so that only a single subscriber
        # gets a request at a time.
        yield from nc.subscribe("help", "workers", cb=request_handler)

        try:
            # Make a request expecting a single response within 500 ms,
            # otherwise raising a timeout error.
            msg = yield from nc.timed_request("help", b'help please', 0.500)
            print("[Response]: {}".format(msg.data))

            # Make a roundtrip to the server to ensure messages
            # that sent messages have been processed already.
            yield from nc.flush(0.500)
        except ErrTimeout:
            print("[Error] Timeout!")

        # Wait a bit for message to be dispatched...
        yield from asyncio.sleep(1, loop=loop)

        # Detach from the server.
        yield from nc.close()

    if nc.last_error is not None:
        print("Last Error: {}".format(nc.last_error))

    if nc.is_closed:
        print("Disconnected.")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(go(loop))
    loop.close()
