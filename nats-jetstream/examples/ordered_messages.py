"""Ordered consumer using messages() iterator.

Demonstrates continuous message consumption with an ordered consumer.
The ordered consumer automatically recovers from sequence gaps, consumer
deletion, and server restarts.

Usage:
    python ordered_messages.py
"""

import asyncio
import signal

from nats import client, jetstream


async def main():
    nc = await client.connect("nats://127.0.0.1:4222")
    js = jetstream.new(nc)

    stream = await js.create_stream(name="TEST_STREAM", subjects=["FOO.*"])

    consumer = await stream.ordered_consumer(
        filter_subjects=["FOO.*"],
        deliver_policy="new",
        max_reset_attempts=5,
    )

    stop = asyncio.Event()
    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, stop.set)

    # Publish messages in the background
    async def publish():
        i = 0
        while not stop.is_set():
            await asyncio.sleep(0.5)
            await js.publish("FOO.TEST1", f"msg {i}".encode())
            i += 1

    task = asyncio.create_task(publish())

    # Consume messages using the async iterator
    messages = await consumer.messages(max_wait=30.0)
    async for msg in messages:
        print(msg.data.decode())
        if stop.is_set():
            break

    await messages.stop()
    task.cancel()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
