import asyncio
import signal
from nats import NATS, Msg


async def main():
    nc = NATS()
    loop = asyncio.get_event_loop()

    async def closed_cb():
        print("Connection to NATS is closed.")
        await asyncio.sleep(0.1)
        loop.stop()

    # It is very likely that the demo server will see traffic from clients other than yours.
    # To avoid this, start your own locally and modify the example to use it.
    options = {
        # "servers": ["nats://127.0.0.1:4222"],
        "servers": ["nats://demo.nats.io:4222"],
        "closed_cb": closed_cb
    }

    await nc.connect(**options)
    print(f"Connected to NATS at {nc.connected_url.netloc}...")

    async def subscribe_handler(msg: Msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print(
            "Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data
            )
        )

    # Basic subscription to receive all published messages
    # which are being sent to a single topic 'discover'
    await nc.subscribe("help", cb=subscribe_handler)

    # Subscription on queue named 'workers' so that
    # one subscriber handles message a request at a time.
    await nc.subscribe("help.*", "workers", subscribe_handler)

    def signal_handler():
        if nc.is_closed:
            return
        print("Disconnecting...")
        loop.create_task(nc.close())

    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    try:
        loop.run_forever()
    finally:
        loop.close()
