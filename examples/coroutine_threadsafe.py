import asyncio
import time
import logging
from threading import Thread
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout

class Component:
    component = None

    def __init__(self):
        self.nc = NATS()
        self.loop = asyncio.new_event_loop()
        if not Component.component:
            Component.component = Component.__Component(self.nc, self.loop)

    def run(self):
        self.loop.run_until_complete(Component.component.run())

        # Without this the ping interval will fail
        self.loop.run_forever()

    def publish(self, subject, data):
        # Required to be able to run the coroutine in the proper thread.
        asyncio.run_coroutine_threadsafe(
            Component.component.publish(subject,data),
            loop=self.loop)

    def request(self, subject, data):
        # Required to be able to run the coroutine in the proper thread.
        future = asyncio.run_coroutine_threadsafe(
            Component.component.request(subject, data),
            loop=self.loop)
        return future.result()

    class __Component:

        def __init__(self, nc, loop):
            self.nc = nc
            self.loop = loop

        async def publish(self, subject, data):
            await self.nc.publish(subject, data)

        async def request(self, subject, data):
            msg = await self.nc.request(subject, data)
            return msg

        async def msg_handler(self, msg):
            print(f"--- Received: {msg.subject} {msg.data} {msg.reply}")
            await self.nc.publish(msg.reply, b'I can help!')

        async def run(self):
            # It is very likely that the demo server will see traffic from clients other than yours.
            # To avoid this, start your own locally and modify the example to use it.
            # await self.nc.connect(servers=["nats://127.0.0.1:4222"], loop=self.loop)
            await self.nc.connect(servers=["nats://demo.nats.io:4222"], loop=self.loop)
            await self.nc.subscribe("help", cb=self.msg_handler)
            await self.nc.flush()

def another_thread(c):
    for i in range(0, 5):
        print("Publishing...")
        c.publish("help", b'hello world')
        time.sleep(1)
        msg = c.request("help", b'hi!')
        print(msg)
        
def go():
    # Create component and have it connect.
    component = Component()

    # Start the component loop in its own thread.
    thr1 = Thread(target=component.run)
    thr1.start()

    # Another thread that will try to publish events
    thr2 = Thread(target=another_thread, args=(component,))
    thr2.start()
    thr2.join()

if __name__ == '__main__':
    go()
