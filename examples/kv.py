import asyncio

import nats


async def main():
    nc = await nats.connect()
    js = nc.jetstream()

    # Create a KV
    kv = await js.create_key_value(bucket='MY_KV')

    # Set and retrieve a value
    await kv.put('hello', b'world')
    entry = await kv.get('hello')
    print(f'KeyValue.Entry: key={entry.key}, value={entry.value}')
    # KeyValue.Entry: key=hello, value=world

    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
