# NATS Client

A Python client for the NATS messaging system.

## Features

- Support for publish/subscribe
- Support for request/reply
- Support for queue groups
- Support for multi-value message headers

## Installation

```bash
pip install nats-client
```

## Usage

```python
import asyncio
from nats.client import connect

async def main():
    client = await connect("nats://localhost:4222")

    # Subscribe
    async with await client.subscribe("foo") as subscription:
        # Publish
        await client.publish("foo", "Hello World!")

        # Receive message
        message = await subscription.next()
        print(f"Received: {message.data}")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```
