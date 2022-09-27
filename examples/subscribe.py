import asyncio
import os
import signal

from nats.aio.client import Client as NATS

from common import args


async def main():
    nc = NATS()

    async def closed_cb():
        print("Connection to NATS is closed.")
        await asyncio.sleep(0.1)
        asyncio.get_running_loop().stop()

    arguments, _ = args.get_args("Run a subscription example.")
    options = {
        "servers": arguments.servers,
        "closed_cb": closed_cb
    }

    await nc.connect(**options)
    print(f"Connected to NATS at {nc.connected_url.netloc}...")

    async def subscribe_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))
        await msg.respond(b'I can help!')

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
        asyncio.create_task(nc.close())

    for sig in ('SIGINT', 'SIGTERM'):
        asyncio.get_running_loop().add_signal_handler(getattr(signal, sig), signal_handler)

    await nc.request("help", b'help')

    await asyncio.sleep(30)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except:
        pass
