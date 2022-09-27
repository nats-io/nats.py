import asyncio
from datetime import datetime

import nats
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrNoServers, ErrTimeout


async def run():

    # Setup pool of servers from a NATS cluster.
    options = {
        "servers": [
            "nats://user1:pass1@127.0.0.1:4222",
            "nats://user2:pass2@127.0.0.1:4223",
            "nats://user3:pass3@127.0.0.1:4224",
        ],
    }

    # Will try to connect to servers in order of configuration,
    # by defaults it connect to one in the pool randomly.
    options["dont_randomize"] = True

    # Optionally set reconnect wait and max reconnect attempts.
    # This example means 10 seconds total per backend.
    options["max_reconnect_attempts"] = 5
    options["reconnect_time_wait"] = 2

    async def disconnected_cb():
        print("Got disconnected!")

    async def reconnected_cb():
        print("Got reconnected to NATS...")

    # Setup callbacks to be notified on disconnects and reconnects
    options["disconnected_cb"] = disconnected_cb
    options["reconnected_cb"] = reconnected_cb

    async def error_cb(e):
        print(f"There was an error: {e}")

    async def closed_cb():
        print("Connection is closed")

    async def subscribe_handler(msg):
        print("Got message: ", msg.subject, msg.reply, msg.data)

    # Setup callbacks to be notified when there is an error
    # or connection is closed.
    options["error_cb"] = error_cb
    options["closed_cb"] = closed_cb

    try:
        nc = await nats.connect(**options)
    except ErrNoServers as e:
        # Could not connect to any server in the cluster.
        print(e)
        return

    if nc.is_connected:
        await nc.subscribe("help.*", cb=subscribe_handler)

        max_messages = 1000
        start_time = datetime.now()
        print(f"Sending {max_messages} messages to NATS...")

        for i in range(0, max_messages):
            try:
                await nc.publish(f"help.{i}", b'A')
                await nc.flush(0.500)
            except ErrConnectionClosed as e:
                print("Connection closed prematurely.")
                break
            except ErrTimeout as e:
                print("Timeout occurred when publishing msg i={}: {}".format(
                    i, e))

        end_time = datetime.now()
        await nc.drain()
        duration = end_time - start_time
        print(f"Duration: {duration}")

        try:
            await nc.publish("help", b"hello world")
        except ErrConnectionClosed:
            print("Can't publish since no longer connected.")

    err = nc.last_error
    if err is not None:
        print(f"Last Error: {err}")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()
