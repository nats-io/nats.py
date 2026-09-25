import asyncio

from nats import client


async def main():
    nc = await client.connect("nats://demo.nats.io")

    # NATS-DOC-START
    # Subscribe to all alarms
    sub_alarm = await nc.subscribe("sensor.alarm.*")

    # Subscribe to all critical
    sub_critical = await nc.subscribe("sensor.*.*.critical")

    # Subscribe to everything under sensor
    sub_all = await nc.subscribe("sensor.>")

    async def reader(sub, label):
        async for msg in sub:
            print(f"[{label:<22}] {msg.data.decode():<15} ({msg.subject})")

    asyncio.create_task(reader(sub_alarm, "sensor.alarm.*"))
    asyncio.create_task(reader(sub_critical, "sensor.*.*.critical"))
    asyncio.create_task(reader(sub_all, "sensor.>"))

    await nc.flush()

    # Publish to specific subjects
    await nc.publish("sensor.alarm.smoke", b"kitchen,14:22")
    await nc.publish("sensor.alarm.smoke.critical", b"kitchen,14:23")
    await nc.publish("sensor.alarm.water", b"basement,16:42")
    await nc.publish("sensor.alarm.water.critical", b"basement,16:43")
    # NATS-DOC-END

    await asyncio.sleep(0.5)
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
