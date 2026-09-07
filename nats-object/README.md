# nats-object

NATS Object Store client for Python implementing [ADR-20](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-20.md). Depends on `nats-jetstream`.

## Usage

```python
import asyncio

import nats.client
import nats.jetstream
import nats.object


async def main() -> None:
    client = await nats.client.connect()
    js = nats.jetstream.new(client)

    store = await nats.object.create_object_store(js, bucket="files")

    info = await store.put("hello.txt", b"hello world")
    print(info.size, info.digest)

    result = await store.get("hello.txt")
    print(await result.read())


asyncio.run(main())
```
