# NATS Key-Value

A Python client for the NATS Key-Value store, built on JetStream.

## Features

- Create, update, and delete buckets
- Put, get, create, update, delete, and purge keys
- Optimistic concurrency via revisions
- Per-key history
- Per-key TTL
- Watch for changes with key pattern filters
- List keys and bucket statuses

## Installation

```bash
pip install nats-key-value
```

Requires Python 3.13+ and a NATS server with JetStream enabled.

## Usage

```python
import asyncio
from nats.client import connect
from nats.jetstream import new as new_jetstream
from nats.key_value import KeyValueConfig, create_or_update_key_value

async def main():
    client = await connect("nats://localhost:4222")
    js = new_jetstream(client)

    kv = await create_or_update_key_value(js, KeyValueConfig(bucket="config"))

    await kv.put("greeting", b"Hello World!")

    entry = await kv.get("greeting")
    print(f"{entry.key} = {entry.value.decode()} (revision {entry.revision})")

    await kv.delete("greeting")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Watching initial values and live updates

Use `values()` to read the finite initial snapshot, then keep watching with
`updates()`. With `ignore_deletes=True`, delete and purge markers are skipped
without ending the watch, including when the initial snapshot ends with one.

```python
async with await kv.watch_all(ignore_deletes=True) as watcher:
    async for entry in watcher.values():
        print(f"Initial: {entry.key} = {entry.value!r}")

    async for entry in watcher.updates():
        print(f"Updated: {entry.key} = {entry.value!r}")
```

## License

MIT

## Contributing

Contributions welcome. Submit a Pull Request on GitHub.
