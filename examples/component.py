import asyncio
import time
import os
import signal

import nats

class Component:
    def __init__(self):
        self._nc = None
        self._done = asyncio.Future()

    async def connect(self, *args, **kwargs):
        self._nc = await nats.connect(*args, **kwargs)

    async def run_forever(self):
        await self._done

    async def close(self):
        await self._nc.close()
        if self._done:
            self._done.cancel()
        asyncio.get_running_loop().stop()

    async def subscribe(self, *args, **kwargs):
        if 'max_tasks' in kwargs:
            sem = asyncio.Semaphore(kwargs['max_tasks'])
            callback = kwargs['cb']

            async def release(msg):
                try:
                    await callback(msg)
                finally:
                    sem.release()

            async def cb(msg):
                await sem.acquire()
                asyncio.create_task(release(msg))

            kwargs['cb'] = cb
      
            # Add some concurrency to the handler.
            del kwargs['max_tasks']

        return await self._nc.subscribe(*args, **kwargs)

    async def publish(self, *args):
        await self._nc.publish(*args)

async def main():
    c = Component()
    await c.connect("nats://localhost:4222")

    async def handler(msg):
        # Cause some head of line blocking
        await asyncio.sleep(0.5)
        print(time.ctime(), f"Received on 'test': {msg.data.decode()}")
      
    async def wildcard_handler(msg):
        print(time.ctime(), f"Received on '>': {msg.data.decode()}")

    await c.subscribe("test", cb=handler, max_tasks=5)
    await c.subscribe(">", cb=wildcard_handler)

    async def producer(label):
        i = 0
        while True:
            # print(time.ctime(), f"Published {i}")
            await c.publish("test", f"{label} :: Hello {i}!".encode())
            await asyncio.sleep(0.2)
            i += 1

    tasks = []
    t = asyncio.create_task(producer("A"))
    tasks.append(t)

    asyncio.create_task(producer("B"))
    tasks.append(t)

    asyncio.create_task(producer("C"))
    tasks.append(t)

    def signal_handler():
        print("Exiting...")
        for task in tasks:
            task.cancel()
        asyncio.create_task(c.close())

    for sig in ('SIGINT', 'SIGTERM'):
        asyncio.get_running_loop().add_signal_handler(getattr(signal, sig), signal_handler)

    await c.run_forever()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except:
        pass
