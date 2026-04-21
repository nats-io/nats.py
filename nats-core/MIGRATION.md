# Migrating from `nats.aio` to `nats.client`

`nats.client` is the modern NATS client. It targets Python 3.13+, uses native async syntax, and models subscriptions as async iterators instead of callbacks.

This guide covers the core publish/subscribe/request surface. JetStream, KV, Object Store, and Micro are not part of `nats.client` and still live in `nats.aio` / `nats.js` / `nats.micro` — applications that depend on those should continue using `nats-py` or combine both clients.

## Install

```bash
pip install nats-core
```

Both packages expose the `nats` namespace; they can be installed side by side during a migration.

## Connecting

### Legacy

```python
import nats

nc = await nats.connect(
    servers=["nats://localhost:4222"],
    name="my-app",
    user="alice",
    password="secret",
    error_cb=on_error,
    reconnected_cb=on_reconnect,
    disconnected_cb=on_disconnect,
)
```

### Modern

```python
from nats.client import connect

client = await connect(
    "nats://localhost:4222",
    user="alice",
    password="secret",
)
client.add_error_callback(on_error)
client.add_reconnected_callback(on_reconnect)
client.add_disconnected_callback(on_disconnect)
```

Differences:

- `connect()` takes a single `url` positional argument instead of a `servers=[...]` list.
- Event callbacks are registered via `add_*_callback` after construction rather than as constructor keyword arguments.
- Returns a `Client` instance usable as an `async with` context manager.

## Publishing

### Legacy

```python
await nc.publish("greet.alice", b"hello")
await nc.publish("greet.alice", b"hello", reply="inbox.1", headers={"k": "v"})
await nc.flush()
```

### Modern

```python
await client.publish("greet.alice", b"hello")
await client.publish("greet.alice", b"hello", reply="inbox.1", headers={"k": "v"})
await client.flush()
```

The signature is the same in practice. `reply` and `headers` are keyword-only in the modern API.

## Subscribing

The biggest shape change: subscriptions are async iterators; there is no `cb=` parameter.

### Legacy (callback)

```python
async def handler(msg):
    print(msg.subject, msg.data)
    await msg.respond(b"ok")

sub = await nc.subscribe("greet.*", cb=handler)
```

### Modern (async iterator)

```python
subscription = await client.subscribe("greet.*")

async for message in subscription:
    print(message.subject, message.data)
    if message.reply:
        await client.publish(message.reply, b"ok")
```

Notes:

- Drive the iterator in a task if you want the handler to run concurrently with other work.
- `subscription.next(timeout=...)` is available for a single-message pull.
- Replying to a request is an explicit `client.publish(message.reply, ...)`; there is no `message.respond()`.

## Queue groups

```python
# Legacy
await nc.subscribe("work", queue="workers", cb=handler)

# Modern
subscription = await client.subscribe("work", queue="workers")
```

## Request / Reply

### Legacy

```python
response = await nc.request("greet.alice", b"ping", timeout=1.0)
```

### Modern

```python
response = await client.request("greet.alice", b"ping", timeout=1.0)
```

Identical for the common case. The modern client raises `NoRespondersError` (a subclass of `StatusError`) when the server reports no responders (503), whereas the legacy client raises `nats.errors.NoRespondersError` from `nats.errors`.

## Headers

Legacy accepts a plain `dict[str, str]`. The modern client uses a dedicated `Headers` class that also accepts a dict.

```python
from nats.client.message import Headers

await client.publish(
    "greet.alice",
    b"hello",
    headers=Headers({"trace-id": "abc", "tags": ["a", "b"]}),
)
```

`Headers` supports `.get(key)`, `.get_all(key)`, `.set(key, value)`, `.delete(key)`, `.append(key, value)`, `.items()`, and `.asdict()`.

## Authentication

### Token

```python
# Legacy
nc = await nats.connect("nats://...", token="secret")

# Modern
client = await connect("nats://...", token="secret")
```

### User / password

```python
# Legacy
nc = await nats.connect("nats://...", user="alice", password="secret")

# Modern
client = await connect("nats://...", user="alice", password="secret")
```

Callable providers (`lambda: fetch_token()`) work in both clients.

### NKey

```python
# Legacy — path to seed file
nc = await nats.connect("nats://...", nkeys_seed="/path/to/user.nk")

# Modern — pass seed as a Path
from pathlib import Path
client = await connect("nats://...", nkey=Path("/path/to/user.nk"))
```

### Decentralized auth (JWT + seed)

```python
# Legacy
nc = await nats.connect(
    "nats://...",
    user_credentials="/path/to/user.creds",
)

# Modern
from pathlib import Path
client = await connect(
    "nats://...",
    jwt=Path("/path/to/user.creds"),
)
```

The modern client accepts the same `.creds` file format or a `(jwt_file, seed_file)` tuple of `Path`s.

## TLS

### Legacy

```python
import ssl

ctx = ssl.create_default_context()
ctx.load_verify_locations("ca.pem")

nc = await nats.connect("tls://nats.example.com:4443", tls=ctx)
```

### Modern

```python
import ssl
from nats.client import connect

ctx = ssl.create_default_context()
ctx.load_verify_locations("ca.pem")

client = await connect("tls://nats.example.com:4443", tls=ctx)
```

`tls_hostname` and `tls_handshake_first` are available in both clients.

## Lifecycle

### Flush / drain / close

```python
# Legacy
await nc.flush()
await nc.drain()
await nc.close()

# Modern
await client.flush()
await client.drain()
await client.close()
```

### Context manager

```python
# Modern only
async with await connect("nats://localhost:4222") as client:
    await client.publish("hello", b"world")
```

## Errors

| Legacy | Modern |
|---|---|
| `nats.errors.Error`, `TimeoutError`, `NoServersError`, `NoRespondersError`, ... | `nats.client.errors.StatusError`, `NoRespondersError`, `SlowConsumerError`, `MaxPayloadError` |
| `await nc.publish(...)` → `nats.errors.OutboundBufferLimitError` on back-pressure | `await client.publish(...)` → `MaxPayloadError` when a payload exceeds server `max_payload` |

`nats.client` does not expose a shared base `Error`; catch `Exception` if you need a generic handler until that gap is closed.

## Message delivery semantics

- Legacy callbacks run inside the read loop. A slow handler slows all incoming traffic on the connection.
- Modern subscriptions deliver to an internal queue (default 65,536 messages / 64 MiB). When the queue fills, a `SlowConsumerError` is reported via `add_error_callback` and subsequent messages are dropped until the consumer catches up.

Tune per-subscription limits:

```python
subscription = await client.subscribe(
    "firehose",
    max_pending_messages=1_000_000,
    max_pending_bytes=512 * 1024 * 1024,
)
```

Pass `None` to either limit to disable that check.

## Things not (yet) in `nats.client`

These stay on `nats-py` for now:

- JetStream (`nc.jetstream()`)
- Key-Value and Object Store (`js.key_value(...)`, `js.object_store(...)`)
- Micro services (`nats.micro`)
- WebSocket transport
- Multi-URL seed lists in `connect()`
- Lame Duck Mode notifications

Applications that need any of these should keep using `nats-py` or run both clients side by side while migrating the pub/sub surface.

## Minimal before/after

```python
# Before — nats-py
import asyncio
import nats

async def main():
    nc = await nats.connect("nats://localhost:4222")

    async def handler(msg):
        await msg.respond(b"pong")

    await nc.subscribe("ping", cb=handler)
    response = await nc.request("ping", b"")
    assert response.data == b"pong"
    await nc.close()

asyncio.run(main())
```

```python
# After — nats-core
import asyncio
from nats.client import connect

async def main():
    async with await connect("nats://localhost:4222") as client:
        subscription = await client.subscribe("ping")

        async def respond():
            async for message in subscription:
                if message.reply:
                    await client.publish(message.reply, b"pong")

        responder = asyncio.create_task(respond())
        try:
            response = await client.request("ping", b"")
            assert response.data == b"pong"
        finally:
            responder.cancel()

asyncio.run(main())
```
