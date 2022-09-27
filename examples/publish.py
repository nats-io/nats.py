import asyncio

import nats

from common import args


async def main():
    arguments, _ = args.get_args("Run a publish example.",
                                 "Usage: python examples/publish.py")
    nc = await nats.connect(arguments.servers)

    # Publish as message with an inbox.
    inbox = nc.new_inbox()
    sub = await nc.subscribe("hello")

    # Simple publishing
    await nc.publish("hello", b'Hello World!')

    # Publish with a reply
    await nc.publish("hello", b'Hello World!', reply=inbox)
    
    # Publish with a reply
    await nc.publish("hello", b'With Headers', headers={'Foo':'Bar'})

    while True:
        try:
            msg = await sub.next_msg()
        except:
            break
        print("----------------------")
        print("Subject:", msg.subject)
        print("Reply  :", msg.reply)
        print("Data   :", msg.data)
        print("Headers:", msg.header)

if __name__ == '__main__':
    asyncio.run(main())
