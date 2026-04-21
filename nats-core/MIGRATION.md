# Migrating from `nats.aio` to `nats.client`

`nats.client` is the modern client for Python 3.13+. This guide only covers the parts that are different from `nats.aio`. Anything not listed (publish, request/reply, flush/drain/close, token / user-password auth, TLS) works the same way.

Both packages share the `nats` namespace and can be installed side by side.

## `connect()` takes one URL

```python
# Before
await nats.connect(servers=["nats://a:4222", "nats://b:4222"])

# After
await connect("nats://a:4222")
```

Multi-URL seed lists aren't supported yet — pick one server; discovered cluster members will still be used for reconnect.

## Event callbacks register after connect

```python
# Before
await nats.connect(
    "nats://...",
    error_cb=on_error,
    disconnected_cb=on_disconnect,
    reconnected_cb=on_reconnect,
)

# After
client = await connect("nats://...")
client.add_error_callback(on_error)
client.add_disconnected_callback(on_disconnect)
client.add_reconnected_callback(on_reconnect)
```

`closed_cb`, `discovered_server_cb`, and `lame_duck_mode_cb` have no equivalent yet.

## Subscriptions are async iterators, not callbacks

```python
# Before
async def handler(msg):
    await msg.respond(b"ok")

await nc.subscribe("greet.*", cb=handler)

# After
subscription = await client.subscribe("greet.*")
async for message in subscription:
    if message.reply:
        await client.publish(message.reply, b"ok")
```

- Drive the iterator in a task if the handler needs to run concurrently with other work.
- There is no `message.respond()`; publish to `message.reply` explicitly.
- `subscription.next(timeout=...)` is available for pull-style consumption.

Subscription delivery is queued (default 65,536 msgs / 64 MiB). When full, a `SlowConsumerError` is reported via the error callback and further messages are dropped. Override per subscription:

```python
await client.subscribe("firehose", max_pending_messages=None, max_pending_bytes=None)
```

## Other `connect()` parameter changes

Renamed:

| Legacy | Modern |
|---|---|
| `connect_timeout` | `timeout` |
| `max_reconnect_attempts` | `reconnect_max_attempts` |
| `dont_randomize` | `no_randomize` |

New reconnect tuning: `reconnect_time_wait_max`, `reconnect_jitter`, `reconnect_timeout`.

Gone (no equivalent yet): `name`, `pedantic`, `verbose`, `pending_size`, `flush_timeout`, `flusher_queue_size`, `drain_timeout`, `ws_connection_headers`, `reconnect_to_server_handler`, `signature_cb` / `user_jwt_cb` (pass handler tuples to `nkey=` / `jwt=` instead).

## NKey and JWT credentials use new parameter names

```python
# Before
await nats.connect("nats://...", nkeys_seed="/path/to/user.nk")
await nats.connect("nats://...", user_credentials="/path/to/user.creds")

# After
from pathlib import Path

await connect("nats://...", nkey=Path("/path/to/user.nk"))
await connect("nats://...", jwt=Path("/path/to/user.creds"))
```

Both accept the same file formats; `jwt=` also accepts a `(jwt_file, seed_file)` tuple of `Path`s.

## Headers have a typed wrapper and support multiple values per key

`client.publish(..., headers=...)` still accepts a plain `dict`, but the `Headers` class is the canonical type and is what arrives on received messages. Unlike the legacy `dict[str, str]`, a single key can carry a list of values:

```python
from nats.client.message import Headers

headers = Headers({"trace-id": "abc", "tags": ["a", "b"]})
headers.append("tags", "c")
headers.get("tags")       # -> "a"          (first value)
headers.get_all("tags")   # -> ["a", "b", "c"]
```

Other methods: `.set(key, value)`, `.delete(key)`, `.items()`, `.asdict()`.

## Error types live in `nats.client.errors`

| Situation | Legacy | Modern |
|---|---|---|
| 503 no responders on request | `nats.errors.NoRespondersError` | `nats.client.errors.NoRespondersError` (subclass of `StatusError`) |
| Subscription can't keep up | reported via `error_cb` | `SlowConsumerError` via `add_error_callback` |
| Payload > server `max_payload` | silent oversize → server disconnect | `MaxPayloadError` raised from `publish()` |

There is no shared base `Error` yet; catch `Exception` for a blanket handler.

## `Client` is an async context manager

```python
async with await connect("nats://localhost:4222") as client:
    await client.publish("hello", b"world")
```

No equivalent on the legacy client; if you used a manual `try/finally: await nc.close()`, you can collapse it.

## Where the rest of the surface lives

- **JetStream** — separate `nats-jetstream` package (layered on top of `nats.client`).
- **Key-Value** — landing in an upcoming `nats-key-value` package.
- **Object Store** — landing in an upcoming `nats-object` package.
- **Services** — landing in an upcoming `nats-service` package (replaces `nats.micro`).

Until the KV, Object Store, and Services packages ship, stay on `nats-py` for those.

Still only in `nats.aio`:

- WebSocket transport
- Multi-URL seed lists in `connect()`
