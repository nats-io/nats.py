import asyncio

import nats


async def main():
    nc = await nats.connect()
    js = nc.jetstream()

    # Create a KV
    kv = await js.create_key_value(bucket="MY_KV")

    # Set and retrieve a value
    await kv.put("hello", b"world")
    await kv.put("goodbye", b"farewell")
    await kv.put("greetings", b"hi")
    await kv.put("greeting", b"hey")

    # Retrieve and print the value of 'hello'
    entry = await kv.get("hello")
    print(f"KeyValue.Entry: key={entry.key}, value={entry.value}")
    # KeyValue.Entry: key=hello, value=world

    # Retrieve keys with filters
    filtered_keys = await kv.keys(filters=["hello", "greet"])
    print(f"Filtered Keys: {filtered_keys}")
    # Expected Output: ['hello', 'greetings']

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
