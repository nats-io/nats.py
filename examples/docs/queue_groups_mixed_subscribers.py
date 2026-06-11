import asyncio

from nats import client


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    async def reader(sub, label):
        async for msg in sub:
            print(f"[{label}] {msg.subject}: {msg.data.decode()}")

    # Audit logger - receives all messages
    audit_sub = await nc.subscribe("orders.>")
    asyncio.create_task(reader(audit_sub, "AUDIT"))

    # Metrics collector - receives all messages
    metrics_sub = await nc.subscribe("orders.>")
    asyncio.create_task(reader(metrics_sub, "METRICS"))

    # Workers in queue group - load balanced
    worker_a_sub = await nc.subscribe("orders.new", queue="new-orders-queue")
    asyncio.create_task(reader(worker_a_sub, "WORKER A"))

    worker_b_sub = await nc.subscribe("orders.new", queue="new-orders-queue")
    asyncio.create_task(reader(worker_b_sub, "WORKER B"))

    await nc.flush()

    # Publish order
    await nc.publish("orders.new", b"Order 123")
    await nc.publish("orders.new", b"Order 124")
    # Audit and metrics see them, one worker processes each
    # NATS-DOC-END

    await asyncio.sleep(0.5)
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
