import asyncio

import nats
from nats.errors import NoServersError, TimeoutError

from common import args


async def main():
    async def disconnected_cb():
        print('Got disconnected!')

    async def reconnected_cb():
        print('Got reconnected to NATS...')

    async def error_cb(e):
        print(f'There was an error: {e}')

    async def closed_cb():
        print('Connection is closed')

    arguments, _ = args.get_args("Run a connect example.")
    nc = await nats.connect(arguments.servers,
                            error_cb=error_cb,
                            reconnected_cb=reconnected_cb,
                            disconnected_cb=disconnected_cb,
                            closed_cb=closed_cb,
                            )

    async def handler(msg):
        print(f'Received a message on {msg.subject} {msg.reply}: {msg.data}')
        await msg.respond(b'OK')
    sub = await nc.subscribe('help.please', cb=handler)
    
    resp = await nc.request('help.please', b'help')
    print('Response:', resp)

if __name__ == '__main__':
    asyncio.run(main())
