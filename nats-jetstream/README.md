# NATS JetStream

A Python client for NATS JetStream, built on `nats-core`. Follows the [JetStream Simplification ADR](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-37.md) — pull-based consumers as the single delivery model, with `fetch`, `messages`, and `next` replacing push subscriptions.

## Features

- Create, update, and delete streams
- Publish with acknowledgements
- Pull and ordered consumers
- Fetch, message streams, and single-message `next()`
- Direct message access (`get_message`, `get_last_message_for_subject`)
- Account info and stream/consumer listing

## Installation

```bash
pip install nats-jetstream
```

Requires Python 3.11+ and a NATS server with JetStream enabled.

## Usage

```python
import asyncio
from nats.client import connect
from nats.jetstream import StreamConfig, new as new_jetstream
from nats.jetstream.consumer import ConsumerConfig

async def main():
    client = await connect("nats://localhost:4222")
    js = new_jetstream(client)

    stream = await js.create_stream(StreamConfig(name="ORDERS", subjects=["orders.*"]))

    await js.publish("orders.new", b"Hello World!")

    consumer = await stream.create_consumer(
        ConsumerConfig(name="worker", ack_policy="explicit"),
    )
    async with consumer:
        message = await consumer.next()
        print(f"Received: {message.data.decode()}")
        await message.ack()

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## License

MIT

## Contributing

Contributions welcome. Submit a Pull Request on GitHub.
