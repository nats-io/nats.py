import asyncio
from threading import Event, Thread
from nats import NATS, Msg


class Component:
    component = None

    def __init__(self):
        self.nc = NATS()
        self.loop = asyncio.new_event_loop()
        self._started = Event()

        if not Component.component:
            Component.component = Component.__Component(self.nc, self._started)

        self._component = Component.component

    def run(self):
        self.loop.run_until_complete(self._component.run())
        self.loop.run_until_complete(self._component.wait_until_closed())

    def publish(self, subject: str, data: bytes) -> None:
        # Required to be able to run the coroutine in the proper thread.
        asyncio.run_coroutine_threadsafe(
            self._component.publish(subject, data), loop=self.loop
        )

    def request(self, subject: str, data: bytes) -> Msg:
        # Required to be able to run the coroutine in the proper thread.
        future = asyncio.run_coroutine_threadsafe(
            self._component.request(subject, data), loop=self.loop
        )
        msg: Msg = future.result()
        return msg

    def wait_for_subscription(self) -> None:
        self._started.wait()

    def close(self) -> None:
        asyncio.run_coroutine_threadsafe(
            self._component.close(), loop=self.loop
        )

    class __Component:
        def __init__(self, nc: NATS, _started: Event):
            self.nc = nc
            self._started = _started

        async def publish(self, subject: str, data: bytes) -> None:
            await self.nc.publish(subject, data)

        async def request(self, subject: str, data: bytes) -> Msg:
            msg = await self.nc.request(subject, data, 1)
            return msg

        async def msg_handler(self, msg: Msg) -> None:
            print(f"--- Received: {msg.subject} {msg.data.decode()}")
            if msg.reply:
                print(f"--- Responding to: {msg.reply}")
                await self.nc.publish(msg.reply, b'I can help!')

        async def run(self) -> None:
            # It is very likely that the demo server will see traffic from clients other than yours.
            # To avoid this, start your own locally and modify the example to use it.
            # await self.nc.connect(servers=["nats://127.0.0.1:4222"])
            await self.nc.connect(servers=["nats://demo.nats.io:4222"])
            await self.nc.subscribe("help", cb=self.msg_handler)
            await self.nc.flush()
            self._started.set()

        async def close(self) -> None:
            await self.nc.close()
            try:
                await self._closed.put(None)
            except AttributeError:
                pass

        async def wait_until_closed(self) -> None:
            self._closed: asyncio.Queue[None] = asyncio.Queue(1)
            await self._closed.get()


def another_thread(c: Component) -> None:
    for i in range(0, 5):
        msg = c.request("help", b'hi!')
        print(msg)


def go():
    # Create component and have it connect.
    component = Component()

    # Start the component loop in its own thread.
    thr1 = Thread(target=component.run)
    thr1.start()
    # Wait for subscription
    component.wait_for_subscription()
    # Another thread that will try to publish events
    thr2 = Thread(target=another_thread, args=(component, ))
    thr2.start()
    thr2.join()

    component.close()


if __name__ == '__main__':
    go()
