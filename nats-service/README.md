# nats-service

NATS service framework for Python implementing [ADR-32](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-32.md).

## Usage

```python
import asyncio

import nats.client
import nats.service


async def main() -> None:
    client = await nats.client.connect()

    async with await nats.service.add_service(
        client,
        name="echo",
        version="0.1.0",
        description="Echoes the request payload back to the caller",
    ) as service:
        async def echo(request: nats.service.Request) -> None:
            await request.respond(request.data)

        await service.add_endpoint(name="echo", handler=echo)
        await service.stopped.wait()


asyncio.run(main())
```
