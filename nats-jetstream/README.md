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

### Inspecting pull-limit warnings

After iterating a batch from `fetch()` or `fetch_nowait()`, inspect
`batch.warning` to distinguish a rejected pull from an idle stream:

```python
batch = await consumer.fetch(max_messages=100, max_wait=1)
async for message in batch:
    await message.ack()
if batch.error is not None:
    raise batch.error
if batch.warning is not None:
    print(batch.warning.code, batch.warning.description)
```

The optional result preserves the server's `409` status and description for
`Exceeded MaxWaiting`, `Exceeded MaxRequestBatch`, `Exceeded MaxRequestExpires`
and `Exceeded MaxRequestMaxBytes`. These warnings conclude the batch without
setting `batch.error` or raising during iteration. Empty streams, request
timeouts and normal byte-limit completion leave `batch.warning` as `None`.
Ordered-consumer batches expose the same result. Continuous `messages()` and
`consume()` streams do not expose this batch property.

## License

MIT

## Contributing

Contributions welcome. Submit a Pull Request on GitHub.
