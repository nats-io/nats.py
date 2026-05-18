# Migrating from `nats.js` to `nats.jetstream`

`nats.jetstream` is the modern JetStream client for Python 3.13+, layered on `nats-core`. This guide only covers the parts that are different from the legacy `nats.js`. Anything not listed (subjects, headers, ack/nak/in_progress/term semantics, consumer config fields like `filter_subject`, `max_deliver`, `max_ack_pending`) works the same way.

All packages share the `nats` namespace and can be installed side by side.

## Getting a JetStream context is a module-level function

```python
# Before
import nats

nc = await nats.connect("nats://localhost:4222")
js = nc.jetstream(timeout=5.0, domain="hub")

# After
from nats.client import connect
from nats.jetstream import new as new_jetstream

client = await connect("nats://localhost:4222")
js = new_jetstream(client, domain="hub")
```

- `nc.jetstream()` is gone — JetStream is no longer attached to the connection. Construct it explicitly via `nats.jetstream.new(client, ...)`.
- `timeout` (per-context API timeout) is gone — pass `timeout=` per call where supported.
- `publish_async_max_pending` is gone — there is no `publish_async` queue; see below.
- New `strict=` flag turns unknown fields in API responses into errors instead of silently dropping them.

## Streams and consumers are first-class objects

```python
# Before
si = await js.add_stream(name="ORDERS", subjects=["orders.*"])
si = await js.stream_info("ORDERS")
msg = await js.get_msg("ORDERS", seq=42)
await js.purge_stream("ORDERS", subject="orders.tmp")
await js.delete_msg("ORDERS", 17)

# After
from nats.jetstream import StreamConfig

stream = await js.create_stream(StreamConfig(name="ORDERS", subjects=["orders.*"]))
info = await stream.get_info()
msg = await stream.get_message(42)
await stream.purge(subject="orders.tmp")
await stream.delete_message(17)
```

`create_stream` / `get_stream` / `update_stream` return a `Stream` object that exposes operations directly — no need to pass the stream name to every call. Same shape for `Consumer` (see below).

Method renames on the JetStream context:

| Legacy | Modern |
|---|---|
| `js.add_stream(...)` | `js.create_stream(StreamConfig(...))` |
| `js.stream_info(name)` | `js.get_stream_info(name)` |
| `js.streams_info_iterator()` | `js.list_streams()` (async iterator, paginated) |
| `js.streams_info()` | `[s async for s in js.list_streams()]` |
| `js.purge_stream(name, ...)` | `(await js.get_stream(name)).purge(...)` |
| `js.get_msg(name, seq=...)` | `(await js.get_stream(name)).get_message(seq)` or `js.get_message(name, seq)` |
| `js.get_last_msg(name, subject)` | `js.get_last_message_for_subject(name, subject)` |
| `js.delete_msg(name, seq)` | `(await js.get_stream(name)).delete_message(seq)` |
| `js.find_stream_name_by_subject(subj)` | `[s async for s in js.stream_names(subject=subj)]` |

Top-level `js.get_message` / `js.get_last_message_for_subject` only work on streams created with `allow_direct=True`; otherwise use the `Stream` methods. New: `Stream.secure_delete_message(seq)` (server-side scrub).

## Durations are `timedelta`, not seconds

```python
# Before
StreamConfig(name="S", subjects=["s.>"], max_age=3600.0, duplicate_window=120.0)
ConsumerConfig(ack_wait=30.0, idle_heartbeat=5.0, inactive_threshold=600.0)

# After
from datetime import timedelta

StreamConfig(
    name="S",
    subjects=["s.>"],
    max_age=timedelta(hours=1),
    duplicate_window=timedelta(minutes=2),
)
ConsumerConfig(
    ack_wait=timedelta(seconds=30),
    idle_heartbeat=timedelta(seconds=5),
    inactive_threshold=timedelta(minutes=10),
)
```

Same swap on `max_expires` and `pause_remaining`. `opt_start_time` / `pause_until` are `datetime`. `StreamInfo.created`, `ConsumerInfo.created`, `StreamMessage.time`, message `metadata.timestamp` are all `datetime` (legacy mixed `int` nanoseconds and `datetime`).

## Config enums are string literals

```python
# Before
from nats.js.api import StreamConfig, RetentionPolicy, StorageType, DiscardPolicy
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy

StreamConfig(retention=RetentionPolicy.WORK_QUEUE, storage=StorageType.MEMORY, discard=DiscardPolicy.NEW)
ConsumerConfig(deliver_policy=DeliverPolicy.NEW, ack_policy=AckPolicy.EXPLICIT)

# After
from nats.jetstream import StreamConfig
from nats.jetstream.consumer import ConsumerConfig

StreamConfig(retention="workqueue", storage="memory", discard="new")
ConsumerConfig(deliver_policy="new", ack_policy="explicit")
```

`StreamConfig` and `ConsumerConfig` are exported from `nats.jetstream`. The enum classes are gone — use the literal strings. `RetentionPolicy.WORK_QUEUE` becomes `"workqueue"` (one word).

## No push consumers — pull only

The legacy `js.subscribe(subject, cb=handler)` push-subscription pattern is gone. Use `Stream.create_consumer(...)` plus `Consumer.fetch()` / `Consumer.messages()` / `Consumer.next()`.

```python
# Before — push with callback
async def handler(msg):
    await msg.ack()
    process(msg)

sub = await js.subscribe("orders.>", durable="worker", cb=handler)

# After — pull, drive the iterator yourself
from nats.jetstream.consumer import ConsumerConfig

consumer = await stream.create_consumer(
    ConsumerConfig(name="worker", filter_subject="orders.>", ack_policy="explicit"),
)
async with consumer:
    async for msg in await consumer.messages():
        await msg.ack()
        process(msg)
```

```python
# Before — explicit pull
psub = await js.pull_subscribe("orders.>", durable="worker")
msgs = await psub.fetch(batch=10, timeout=5)

# After
batch = await consumer.fetch(max_messages=10, max_wait=5)
async for msg in batch:
    ...
```

Renames on the consumer side:

| Legacy | Modern |
|---|---|
| `psub.fetch(batch=N, timeout=T)` | `consumer.fetch(max_messages=N, max_wait=T)` |
| `psub.fetch(...)` returning `List[Msg]` | `consumer.fetch(...)` returning a `MessageBatch` async iterator |
| `sub.next_msg(timeout=T)` | `consumer.next(max_wait=T)` |
| `js.subscribe(..., ordered_consumer=True)` | `js.ordered_consumer(stream, OrderedConsumerConfig(...))` or `stream.ordered_consumer(...)` |

New: `Consumer.fetch_nowait(...)` (server returns immediately if nothing pending) and `Consumer.reset(seq=...)` (ADR-60). `Consumer` is an async context manager — `async with consumer:` replaces the manual `await sub.unsubscribe()`.

`MaxBytesError`, flow-control sync, and heartbeat handling are managed inside `consumer.messages(...)` / `fetch(...)`; you don't see them as separate parameters on push subscriptions.

## Message acks: `ack_sync` → `double_ack`, `nak(delay)` split

```python
# Before
await msg.ack_sync(timeout=1.0)
await msg.nak()              # immediate redelivery
await msg.nak(delay=2.0)     # redelivery after 2s
await msg.term()

# After
await msg.double_ack(timeout=1.0)
await msg.nak()              # immediate redelivery
await msg.nak_with_delay(2.0)
await msg.term()
await msg.term_with_reason("poison pill")  # NATS 2.10.4+
```

`msg.ack()` / `msg.in_progress()` are unchanged. `msg.metadata` carries the same fields (`sequence.consumer`, `sequence.stream`, `num_delivered`, `num_pending`, `timestamp`, `stream`, `consumer`, `domain`) — `timestamp` is always a `datetime` in modern.

## `publish()` shape changed; no `publish_async`

```python
# Before
ack = await js.publish("orders.new", b"...", stream="ORDERS", timeout=2.0, msg_ttl=60.0)
ack.seq           # int
ack.duplicate     # bool

# After
ack = await js.publish(
    "orders.new",
    b"...",
    headers={"Nats-Expected-Stream": "ORDERS", "Nats-TTL": "1m"},
    timeout=2.0,
)
ack.sequence      # int (was `seq`)
ack.duplicate     # bool
ack.value         # str | None  (counter streams, ADR-49)
ack.batch_id      # str | None  (atomic batch publish, ADR-50)
ack.batch_size    # int | None
```

- Return type renamed: `PubAck` → `PublishAck`. Field `seq` → `sequence`. New: `value`, `batch_id`, `batch_size`.
- `stream=` (expected stream) and `msg_ttl=` are gone — set the corresponding headers explicitly: `Nats-Expected-Stream`, `Nats-TTL`.
- New `retry_attempts` / `retry_wait` kwargs auto-retry on `NoRespondersError` (handy during cluster leader transitions).
- `publish_async(...)`, `publish_async_pending()`, `publish_async_completed()` have no equivalent yet. If you batched async publishes for throughput, run a bounded `asyncio.gather` over `publish()` calls instead.

## Errors live in `nats.jetstream.errors`

```python
# Before
from nats.js.errors import APIError, NotFoundError, ServiceUnavailableError, BadRequestError

try:
    await js.stream_info("MISSING")
except NotFoundError:
    ...

# After
from nats.jetstream import JetStreamError, StreamNotFoundError, ErrorCode

try:
    await js.get_stream_info("MISSING")
except StreamNotFoundError:
    ...
```

| Situation | Legacy | Modern |
|---|---|---|
| Generic JS API error | `nats.js.errors.APIError` | `nats.jetstream.errors.JetStreamError` |
| Stream missing | `nats.js.errors.NotFoundError` | `StreamNotFoundError` |
| Stream name in use | `nats.js.errors.BadRequestError` (10058) | `StreamNameAlreadyInUseError` |
| Consumer missing | `nats.js.errors.NotFoundError` | `ConsumerNotFoundError` |
| Message missing | `nats.js.errors.NotFoundError` | `MessageNotFoundError` |
| JetStream not enabled | `nats.js.errors.APIError` (10076) | `JetStreamNotEnabledError` |
| JS not enabled for account | `nats.js.errors.APIError` (10039) | `JetStreamNotEnabledForAccountError` |
| Max consumers reached | `nats.js.errors.APIError` (10026) | `MaximumConsumersLimitError` |
| No responders on publish | `nats.js.errors.NoStreamResponseError` | `nats.client.errors.NoRespondersError` |

The generic `NotFoundError` / `BadRequestError` / `ServiceUnavailableError` / `ServerError` hierarchy is gone — catch the specific subclass, or the `JetStreamError` base. `JetStreamError.code` (HTTP-style status) and `JetStreamError.error_code` (numeric JS code) are exposed for the long tail; constants live on `ErrorCode` (e.g. `ErrorCode.STREAM_NOT_FOUND == 10059`).

## Where the rest of the surface lives

- **Key-Value** — separate `nats-key-value` package; see its `MIGRATION.md`.
- **Object Store** — separate `nats-object` package.
- **Services** — separate `nats-service` package (replaces `nats.micro`).

Until those packages cover what you need, stay on `nats-py` for those bits — `nats.js`, `nats.jetstream`, and `nats.key_value` can be imported side by side in the same process.
