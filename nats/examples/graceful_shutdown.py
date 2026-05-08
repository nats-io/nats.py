import asyncio
import signal

import nats


async def main():
    # Connect to NATS
    nc = await nats.connect("nats://127.0.0.1:4222")

    # Use an event to coordinate graceful shutdown
    shutdown = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    async def handler(msg):
        print(f"Received: {msg.data.decode()} on {msg.subject}")

    await nc.subscribe("foo", cb=handler)

    # Publish messages until shutdown is signalled
    while not shutdown.is_set():
        await nc.publish("foo", b"bar")
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=1)
        except asyncio.TimeoutError:
            pass

    # Drain gracefully closes the connection after flushing
    await nc.drain()
    print("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
