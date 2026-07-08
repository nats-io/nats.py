import asyncio

from nats import client


# NATS-DOC-START
class Worker:
    def __init__(self, worker_id):
        self.id = worker_id
        self.subscription = None
        self.task = None

    async def start(self, nc, subject, queue):
        self.subscription = await nc.subscribe(subject, queue=queue)
        self.task = asyncio.create_task(self._run())

    async def _run(self):
        async for msg in self.subscription:
            print(f"Worker {self.id} processing: {msg.data.decode()}")

    async def stop(self):
        await self.subscription.unsubscribe()


async def main():
    nc = await client.connect("nats://demo.nats.io")

    workers = []
    subject = "tasks"
    queue = "workers"

    # Scale up
    for i in range(1, 6):
        w = Worker(i)
        await w.start(nc, subject, queue)
        workers.append(w)

    # Scale down
    for w in workers:
        await w.stop()

    await nc.close()
# NATS-DOC-END


if __name__ == "__main__":
    asyncio.run(main())
