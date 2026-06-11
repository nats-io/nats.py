import asyncio

from nats import client
from nats.client.errors import NoRespondersError


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    async def service_instance(sub, instance_id):
        async for msg in sub:
            parts = msg.data.decode().split(",")
            result = int(parts[0]) + int(parts[1])
            response = f"Result: {result}, processed by: instance-{instance_id}"
            if msg.reply:
                await nc.publish(msg.reply, response.encode())
            print(f"Instance instance-{instance_id} processed request")

    # Set up 3 instances of the service
    for i in range(1, 4):
        sub = await nc.subscribe("api.calculate", queue="api-workers-queue")
        asyncio.create_task(service_instance(sub, i))

    await nc.flush()

    # Make requests - messages are balanced among the subscribers in the queue
    for i in range(10):
        data = f"{i},{i * 2}"
        try:
            m = await nc.request("api.calculate", data.encode(), timeout=0.5)
            print(m.data.decode())
        except (TimeoutError, NoRespondersError):
            print(f"{i}) No Response")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
