# Migrating from `nats.js` KV to `nats.key_value`

`nats.key_value` is the modern Key-Value client for Python 3.13+, layered on `nats-core` and `nats-jetstream`. This guide only covers the parts that are different from the legacy `nats.js` KV API. Anything not listed (`put`, `get`, `create`, `update`, `delete`, `purge`, optimistic concurrency via revisions) works the same way.

All packages share the `nats` namespace and can be installed side by side.

## Getting a KV handle is a module-level function

```python
# Before
import nats

nc = await nats.connect("nats://localhost:4222")
js = nc.jetstream()

kv = await js.create_key_value(bucket="config", history=5)
# or
kv = await js.key_value("config")

# After
from nats.client import connect
from nats.jetstream import new as new_jetstream
from nats.key_value import KeyValueConfig, create_key_value, key_value

client = await connect("nats://localhost:4222")
js = new_jetstream(client)

kv = await create_key_value(js, KeyValueConfig(bucket="config", history=5))
# or
kv = await key_value(js, "config")
```

`KeyValueConfig` is required for create/update; kwargs aren't accepted. Bucket lifecycle is now: `create_key_value`, `update_key_value`, `create_or_update_key_value`, `key_value` (bind), `delete_key_value`. Listing helpers `key_value_bucket_names(js)` and `key_value_buckets(js)` are new — there is no equivalent on the legacy `JetStreamContext`.

## Durations are `timedelta`, not seconds

```python
# Before
await js.create_key_value(bucket="cache", ttl=60.0)           # seconds
await kv.create("token", b"...", msg_ttl=30.0)                # seconds
await kv.purge_deletes(olderthan=300)                         # seconds (int)

# After
from datetime import timedelta

await create_key_value(js, KeyValueConfig(
    bucket="cache",
    ttl=timedelta(seconds=60),
    limit_marker_ttl=timedelta(minutes=5),  # required for per-key TTL
))
await kv.create("token", b"...", ttl=timedelta(seconds=30))
await kv.purge_deletes(older_than=timedelta(minutes=5))
```

`KeyValueStatus.ttl` and `.limit_marker_ttl` are also `timedelta | None`. The `msg_ttl` parameter is renamed to `ttl` on `create()` and `purge()`; `delete()` no longer takes a TTL parameter. Per-key TTL requires `limit_marker_ttl` on the bucket — the legacy client silently allowed `msg_ttl` even when the bucket couldn't honor it.

## `KeyValueConfig` is a top-level dataclass

```python
# Before
from nats.js.api import KeyValueConfig, StorageType

KeyValueConfig(bucket="b", storage=StorageType.MEMORY, direct=True)

# After
from nats.key_value import KeyValueConfig

KeyValueConfig(bucket="b", storage="memory")
```

| Legacy | Modern |
|---|---|
| `nats.js.api.KeyValueConfig` | `nats.key_value.KeyValueConfig` |
| `storage: StorageType \| None` | `storage: Literal["file", "memory"] \| None` |
| `direct: bool \| None` | _removed — direct gets are always used_ |
| `ttl: float \| None` (seconds) | `ttl: timedelta \| None` |

Added: `compression: bool`, `mirror`, `sources`, `limit_marker_ttl: timedelta | None`, `metadata: dict[str, str] | None`.

## `Entry` is a top-level dataclass with a typed `operation`

```python
# Before
entry = await kv.get("k")
entry.operation  # None for PUT, "DEL" or "PURGE" for tombstones (str | None)
entry.created    # int | None (nanoseconds since epoch)

# After
from nats.key_value import KeyValueOp

entry = await kv.get("k")
entry.operation  # KeyValueOp.PUT / .DELETE / .PURGE (always set)
entry.created    # datetime (timezone-aware UTC)
```

The class moved from `KeyValue.Entry` (nested) to `nats.key_value.KeyValueEntry` (top-level), and `revision`, `delta`, `created`, `operation` are all non-`Optional` now. `get()` still raises `KeyNotFoundError` for tombstoned keys; you only see `DELETE`/`PURGE` entries when iterating history or a watcher.

## `BucketStatus` → `KeyValueStatus`

```python
# Before
status = await kv.status()
status.bucket         # str
status.values         # int
status.history        # int
status.ttl            # float | None (seconds)
status.stream_info    # nats.js.api.StreamInfo

# After
status = await kv.status()
status.bucket            # str
status.values            # int
status.history           # int
status.ttl               # timedelta | None
status.bytes             # int                — new
status.backing_store     # str                — new ("JetStream")
status.compressed        # bool               — new
status.limit_marker_ttl  # timedelta | None   — new
status.metadata          # dict[str,str] | None — new
status.stream_info       # nats.jetstream.StreamInfo
```

`KeyValueStatus` is a plain dataclass (the legacy `BucketStatus` is `frozen=True` with property accessors that read from `stream_info`).

## Watchers iterate, no `None` sentinel

```python
# Before
watcher = await kv.watchall()
async for entry in watcher:
    if entry is None:
        break  # end of initial values; further iterations are live updates
    handle(entry)
await watcher.stop()

# After
async with await kv.watch_all() as watcher:
    async for entry in watcher.values():    # finishes at end of initial values
        handle(entry)
    async for entry in watcher.updates():   # then live updates
        handle(entry)
```

- `watchall()` → `watch_all()`.
- The `None` end-of-initial-values marker is gone; use `watcher.at_eod()` or the `.values()` / `.updates()` helpers.
- `KeyWatcher` is an async context manager — replaces manual `await watcher.stop()`.
- `watcher.updates(timeout=...)` is gone; iterate directly or wrap `__anext__()` in `asyncio.wait_for`.
- New `watch_filtered(keys: list[str], ...)` for multi-pattern subscriptions.
- New options on `watch()`: `updates_only=True` (skip initial values) and `resume_from_revision=...` (start at a specific sequence).
- The legacy `headers_only` and `inactive_threshold` parameters are gone; use `meta_only=True` for headers-only delivery.

## `keys()` and `history()` return async iterators

```python
# Before
names = await kv.keys()                       # List[str], raises NoKeysError when empty
entries = await kv.history("config")          # List[Entry], raises NoKeysError when empty

# After
lister = await kv.keys()                      # KeyLister, empty iterator when no keys
async for name in lister:
    ...

history = await kv.history("config")          # KeyHistory, empty iterator when no entries
async for entry in history:
    ...
```

`keys()` accepts a pattern directly: `await kv.keys("user.*")` (wildcards and `>` supported). The legacy `filters=[...]` substring-match parameter is gone — use subject wildcards instead. Empty buckets yield no entries rather than raising; `NoKeysError` no longer exists.

## Return types tightened

| Method | Legacy | Modern |
|---|---|---|
| `delete(key, ...)` | returns `bool` (always `True`) | returns `None` |
| `purge(key, ...)` | returns `bool` (always `True`) | returns `None` |
| `purge_deletes(...)` | returns `bool` (always `True`) | returns `None` |
| `delete_key_value(bucket)` | returns `bool` | returns `None` |
| `update(key, value, last)` | `last` defaults to `None`/`0` | `revision: int` is required positional |

`delete()` / `purge()` take `last_revision` (keyword-only) instead of `last`. The `validate_keys=False` escape hatch is gone — keys are always validated.

## Errors live in `nats.key_value.errors`

| Situation | Legacy | Modern |
|---|---|---|
| Key missing or tombstoned | `nats.js.errors.KeyNotFoundError` | `nats.key_value.errors.KeyNotFoundError` |
| `create()` on existing key | `nats.js.errors.KeyWrongLastSequenceError` | `nats.key_value.errors.KeyExistsError` |
| `update()` with wrong revision | `nats.js.errors.KeyWrongLastSequenceError` | `nats.key_value.errors.KeyExistsError` |
| Invalid key | `nats.js.errors.InvalidKeyError` | `nats.key_value.errors.InvalidKeyError` |
| Invalid bucket name | `nats.js.errors.InvalidBucketNameError` | `nats.key_value.errors.InvalidBucketNameError` |
| Bucket missing | `nats.js.errors.BucketNotFoundError` | `nats.key_value.errors.BucketNotFoundError` |
| Stream isn't a KV | `nats.js.errors.BadBucketError` | `nats.key_value.errors.BadBucketError` |
| `history > 64` | `nats.js.errors.KeyHistoryTooLargeError` | `nats.key_value.errors.HistoryTooLargeError` |
| `create_key_value` on existing bucket | (none — succeeded silently) | `nats.key_value.errors.BucketExistsError` |

All errors share a `KeyValueError` base. `KeyDeletedError` is gone: `get()` raises `KeyNotFoundError` for tombstones, and watchers/history surface tombstones as entries with `operation` set to `KeyValueOp.DELETE` or `KeyValueOp.PURGE`.
