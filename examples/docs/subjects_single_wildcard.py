import asyncio

from nats import client


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Subscribe to shipped orders
    sub_shipped = await nc.subscribe("orders.*.shipped")

    # Subscribe to placed orders
    sub_placed = await nc.subscribe("orders.*.placed")

    # Subscribe to retail orders
    sub_retail = await nc.subscribe("orders.retail.*")

    async def reader(sub, label):
        async for msg in sub:
            print(f"[{label:<18}] {msg.data.decode()}  ({msg.subject})")

    asyncio.create_task(reader(sub_shipped, "orders.*.shipped"))
    asyncio.create_task(reader(sub_placed, "orders.*.placed"))
    asyncio.create_task(reader(sub_retail, "orders.retail.*"))

    await nc.flush()

    # Publish to specific subjects
    await nc.publish("orders.wholesale.placed", b"Order W73737")
    await nc.publish("orders.retail.placed", b"Order R65432")
    await nc.publish("orders.wholesale.shipped", b"Order W73001")
    await nc.publish("orders.retail.shipped", b"Order R65321")
    # NATS-DOC-END

    await asyncio.sleep(0.5)
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
