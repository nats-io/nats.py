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

The current default is **60 failed attempts per server**. Set `reconnect_max_attempts=0` for unlimited attempts. Removed servers retain their counts, and newly discovered servers inherit the highest active count. The first reconnect starts immediately; later passes use bounded exponential backoff with jitter.

Type narrowed: `inbox_prefix` is now `str` only (legacy accepted `str | bytes`).

Broadened: `token`, `user`, and `password` now also accept a zero-argument callable that returns the credential — useful for rotating secrets.

```python
await connect("nats://...", token=lambda: read_token_from_vault())
```

New reconnect tuning: `reconnect_time_wait_max`, `reconnect_jitter`, `reconnect_timeout`.

Supported: `name`, `pedantic`, and `verbose`.

Gone (no equivalent yet): `pending_size`, `flush_timeout`, `flusher_queue_size`, `drain_timeout`, `ws_connection_headers`, `reconnect_to_server_handler`.

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

Both accept the same file formats; `jwt=` also accepts a `(jwt_file, seed_file)` tuple of `Path`s. If you previously supplied custom `signature_cb` / `user_jwt_cb` callbacks, pass them as a handler tuple to the same parameters: `nkey=(public_key_cb, signature_cb)` or `jwt=(jwt_cb, signature_cb)`.

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

Both WebSocket transport (`pip install nats-core[websocket]`) and multi-URL seed lists are supported by `nats.client.connect()`.

Connection setup now has one timeout budget through the first PONG, defaulting to 5 seconds. A TLS-after-INFO connection to nats-server 2.12 or later can wait up to one additional timeout for its post-PONG INFO. Set `ignore_discovered_servers=True` to use only configured seeds and skip that wait. Older servers may require configuring multiple seeds or using `tls_handshake_first=True` to retain initial failover targets.

URL user/password or token credentials are percent-decoded and override credential options and bare NKey authentication. JWT/credentials-file authentication remains configured independently. Remove stale userinfo from URLs if you intend to use option credentials.

TLS settings request TLS for the whole configured list unless `tls_optional=True`. Under that opt-out, explicit `tls://` and `wss://` seeds still use TLS. `tls_required=True` and `tls_handshake_first=True` conflict with `tls_optional=True` and are rejected before connecting. Use `tls_hostname` for certificate-name mismatches. Disabling verification requires the explicit `tls_skip_verify=True` switch, even with a user-supplied context that disables verification. Supplied contexts are never modified: with `tls_skip_verify=True`, a custom context must already have `check_hostname=False` and `verify_mode=ssl.CERT_NONE`. Omit `tls` to let the client create its own unverified context.
