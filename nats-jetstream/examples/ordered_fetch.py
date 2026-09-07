"""Ordered consumer using fetch() batches.

Demonstrates batch message consumption with an ordered consumer.
Each fetch call retrieves a batch of messages. The ordered consumer
automatically recreates the underlying server-side consumer between
fetches if needed.

Usage:
    python ordered_fetch.py
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

    # Fetch messages in batches
    while not stop.is_set():
        batch = await consumer.fetch(max_messages=100, max_wait=5.0)
        async for msg in batch:
            print(msg.data.decode())
        if batch.error is not None:
            print(f"Error fetching messages: {batch.error}")

    task.cancel()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
