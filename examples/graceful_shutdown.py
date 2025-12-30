import asyncio
import signal
import sys

import nats


def sig_cb(shutdown_event: asyncio.Event):
    print("received shutdown signal")
    # Setting shutdown event to true, so other listeners can close
    shutdown_event.set()


async def nats_consumer(shutdown_event: asyncio.Event):
    # Connect to NATS!
    try:
        nc = await nats.connect(
            servers=["nats://127.0.0.1:4222"],
            # Setting max rc attempts in order to prevent termination block
            max_reconnect_attempts=3,
        )
    except Exception as e:
        print(e)
        return

    # Callback for sub
    async def cb(msg):
        print("received: ", msg)

    # Receive messages on 'foo'
    await nc.subscribe("foo", cb=cb)

    # Wait until shutdown event is set to release resources
    await shutdown_event.wait()

    await nc.drain()
    print("gracefully closed nats consumer")


async def nats_producer(shutdown_event: asyncio.Event):
    # Connect to NATS!
    try:
        nc = await nats.connect(
            servers=["nats://127.0.0.1:4222"],
            # Setting max rc attempts in order to prevent termination block
            max_reconnect_attempts=3,
        )
    except Exception as e:
        print(e)
        return

    while not shutdown_event.is_set():
        await nc.publish("foo", b"bar")
        await asyncio.sleep(1)

    await nc.drain()
    print("gracefully closed nats producer")


async def main(shutdown_event: asyncio.Event):
    nats_consumer_task = asyncio.create_task(nats_consumer(shutdown_event))
    nats_producer_task = asyncio.create_task(nats_producer(shutdown_event))

    await asyncio.gather(*[nats_consumer_task, nats_producer_task])

    sys.exit(0)


if __name__ == "__main__":
    # Creating shutdown event to notify child routines of a shutdown
    shutdown_event = asyncio.Event()
    try:
        loop = asyncio.new_event_loop()
        # Setting signals handlers to catch termination and interruption events
        loop.add_signal_handler(signal.SIGTERM, sig_cb, shutdown_event)
        loop.add_signal_handler(signal.SIGINT, sig_cb, shutdown_event)
        loop.run_until_complete(main(shutdown_event))
        loop.close()
    except KeyboardInterrupt:
        print("interrupted by ctrl+c")
