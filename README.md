# NATS - Python Clients and Tools

Python clients and tooling for the [NATS messaging system](https://nats.io).

- [nats-core](#nats-core) — Core NATS client with pub/sub, request/reply, reconnection, and auth
- [nats-jetstream](#nats-jetstream) — JetStream streams, consumers, and publish with ack
- [nats-server](#nats-server) — Manage NATS server instances from Python for dev/test
- [nats-py](#nats-py-legacy) — Stable legacy client with built-in JetStream, KV, and object store

**nats-core** and **nats-jetstream** are the actively developed packages — faster (48x–267x depending on message size, [benchmarks](https://github.com/nats-io/nats.py/pull/732)) with modern Python APIs. **nats-jetstream** follows the [JetStream simplification ADR](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-37.md).

**nats-py** is still stable, maintained and has been in production use for years.

## nats-core

Core NATS client with support for publish/subscribe, request/reply, queue groups, headers, automatic reconnection, TLS, and NKey/JWT authentication. Significantly faster than nats-py ([benchmarks](https://github.com/nats-io/nats.py/pull/732)).

```python
import asyncio
from nats.client import connect

async def main():
    client = await connect("nats://localhost:4222")

    # Subscribe with wildcard
    async with await client.subscribe("greet.*") as subscription:
        await client.publish("greet.world", b"Hello!")

        message = await subscription.next()
        print(f"Received on {message.subject}: {message.data}")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

See the [nats-core README](./nats-core) for more details.

## nats-jetstream

JetStream client built on nats-core, with support for stream and consumer management, publish with acknowledgement, pull consumers, and ordered consumers.

```python
import asyncio
from nats.client import connect
from nats.jetstream import new as new_jetstream

async def main():
    client = await connect("nats://localhost:4222")
    js = new_jetstream(client)

    # Create a stream
    stream = await js.create_stream(name="EVENTS", subjects=["events.>"])

    # Publish with acknowledgement
    ack = await js.publish("events.page_loaded", b"user123")
    print(f"Published to stream: {ack.stream}, seq: {ack.sequence}")

    # Create a pull consumer and fetch messages
    consumer = await stream.create_consumer(name="my-consumer")
    batch = await consumer.fetch(max_messages=10, max_wait=1.0)

    async for msg in batch:
        print(f"Received: {msg.data}")
        await msg.ack()

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

See the [nats-jetstream README](./nats-jetstream) for more details.

## nats-server

Manage NATS server instances from Python for development and testing.

```python
import asyncio
import nats.server

async def main():
    server = await nats.server.run(port=0, jetstream=True)
    print(f"Server running on {server.client_url}")

    # ... use the server ...

    await server.shutdown()

asyncio.run(main())
```

See the [nats-server README](./nats-server) for more details.

## nats-py (legacy)

The stable, production-tested NATS client with broad Python version support (3.7+) and built-in JetStream, Key-Value, and Object Store support.

```python
import asyncio
import nats

async def main():
    nc = await nats.connect("nats://localhost:4222")

    # Subscribe with callback
    async def handler(msg):
        print(f"Received on '{msg.subject}': {msg.data.decode()}")

    sub = await nc.subscribe("foo", cb=handler)

    await nc.publish("foo", b"Hello")
    await nc.publish("foo", b"World")

    # Request/Reply
    response = await nc.request("help", b"please", timeout=0.5)

    # JetStream
    js = nc.jetstream()
    await js.add_stream(name="sample", subjects=["orders.>"])
    ack = await js.publish("orders.new", b"order-1")

    await nc.drain()

if __name__ == "__main__":
    asyncio.run(main())
```

See the [nats-py documentation](https://nats-io.github.io/nats.py/) and [package README](./nats) for more details.

## Client and Orbit

NATS client functionality is split across two layers: the **core client**
(`nats-py`, this repo) and **[Orbit](https://github.com/synadia-io/orbit.py)**,
a separate set of packages with higher-level utilities.

The split exists so the core can stay small, stable, and consistent across
NATS clients in every language, while Orbit can iterate quickly on
opinionated abstractions without dragging the core API along for the ride.

### Core client (`nats-py`)

- Direct API over Core NATS and JetStream as exposed by `nats-server`.
- Lightweight, unopinionated, performance-oriented.
- API surface kept in **parity** with other official NATS clients
  (Rust, Go, .NET, Java, JS, C). A feature shipped here should look
  the same shape everywhere.
- Stable, conservative versioning. Breaking changes are rare and deliberate.

### Orbit (`orbit.py`)

- Higher-level, opinionated abstractions built **on top of** the core client.
- Per-package versioning, so an experimental utility can iterate
  without bumping every other piece.
- Free to be language-specific: a Python-idiomatic API does not need to match
  the equivalent in other languages.
- May lag, omit, or extend cross-client parity items.

### What goes where?

| Concern                                            | Core (`nats-py`)    | Orbit |
|----------------------------------------------------|:-------------------:|:-----:|
| Connect, publish, subscribe, request/reply         | ✅                  |       |
| JetStream publish, consumers, streams, KV, OS      | ✅                  |       |
| Service API (request/reply micro-services)         | ✅                  |       |
| Wire-protocol coverage, auth, TLS, reconnection    | ✅                  |       |
| Cross-client parity, conservative semver           | ✅                  |       |
| Opinionated helpers / sugar over core APIs         |                     | ✅    |
| New experimental patterns (e.g. partitioned groups)|                     | ✅    |
| KV codecs, distributed counters, NATS contexts     |                     | ✅    |
| Python-idiomatic abstractions with no parity mandate|                    | ✅    |
| Per-utility versioning, faster API churn allowed   |                     | ✅    |

> **Rule of thumb:** if it is a thin mapping of something `nats-server`
> already speaks and every official client must expose it, it belongs in
> core. If it is a pattern, helper, or abstraction layered on top, it
> belongs in Orbit.

### Layering

```text
   ┌──────────────────────────────────────────────────────┐
   │  Application code                                    │
   └──────────────┬───────────────────────────┬───────────┘
                  │                           │
                  ▼                           ▼
        ┌───────────────────┐       ┌───────────────────┐
        │ Orbit packages    │  uses │ nats-py (core)    │
        │ (opinionated,     │──────▶│ (parity, stable,  │
        │  per-pkg semver)  │       │  protocol-level)  │
        └───────────────────┘       └─────────┬─────────┘
                                              │
                                              ▼
                                       ┌─────────────┐
                                       │ nats-server │
                                       └─────────────┘
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

Packages are licensed under Apache 2.0 and/or MIT. See individual packages for license information.
