import asyncio
import time
from threading import Thread
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout


class Component(object):
    def __init__(self, nc, loop):
        self.nc = nc
        self.loop = loop

    def response_handler(self, msg):
        print("--- Received: ", msg.subject, msg.data)

    async def another_handler(self, msg):
        print("--- Another: ", msg.subject, msg.data, msg.reply)
        await self.nc.publish(msg.reply, b'I can help!')

    def run(self):
        await self.nc.connect(io_loop=self.loop)
        await self.nc.subscribe("hello", cb=self.response_handler)
        await self.nc.subscribe("another", cb=self.another_handler)
        await self.nc.flush()


def another_thread(c):
    # Should have ensured that we are connected by this point.
    if not c.nc.is_connected:
        print("Not connected to NATS!")
        return

    asyncio.run_coroutine_threadsafe(
        c.nc.subscribe("hi", cb=c.response_handler), loop=c.loop)
    asyncio.run_coroutine_threadsafe(c.nc.flush(), loop=c.loop)
    asyncio.run_coroutine_threadsafe(
        c.nc.publish("hello", b'world'), loop=c.loop)
    asyncio.run_coroutine_threadsafe(
        c.nc.publish("hi", b'example'), loop=c.loop)

    future = asyncio.run_coroutine_threadsafe(
        c.nc.timed_request("another", b'example'), loop=c.loop)
    msg = future.result()
    print("--- Got: ", msg.data)


def go():
    # Starting the NATS client in this thread...
    nc = NATS()
    loop = asyncio.get_event_loop()
    component = Component(nc, loop)

    # Wait for coroutine to finish and ensure we are connected
    loop.run_until_complete(component.run())

    # Example using NATS client from another thread.
    thr = Thread(target=another_thread, args=(component, ))
    thr.start()

    loop.run_forever()


if __name__ == '__main__':
    go()
