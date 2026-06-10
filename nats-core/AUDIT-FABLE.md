# nats-core Audit (Fable)

Full-source review of `nats-core/src/nats/client/` (~3,500 lines): correctness issues,
parity defects against expected NATS client behavior, and unpythonic patterns.
File references are relative to `nats-core/src/nats/client/`.

---

## Critical

### C1. Top-level `import nkeys` breaks the package without the optional extra

`__init__.py:40` imports `nkeys` unconditionally, but `pyproject.toml` declares
`dependencies = []` and ships nkeys only as the `nkeys` extra. A plain
`pip install nats-core` followed by `import nats.client` raises `ImportError`.
The import should be deferred into `_setup_nkey_auth` / `_setup_jwt_auth` (the
only consumers), with a clear error message pointing at `nats-core[nkeys]`,
mirroring how the websocket extra is handled in `connection.py:317-321`.

### C2. Ping state is not reset on reconnect → reconnect death spiral

`__init__.py:960-997` (reconnect success path) restores subscriptions and stats
but never resets `_pings_outstanding`, `_last_ping_sent`, or `_last_pong_received`.
If the original disconnect was caused by reaching `max_outstanding_pings`
(`_pings_outstanding == 2`), the value survives into the new connection. The new
`_write_loop` then hits its first idle timeout, sees
`_pings_outstanding >= _max_outstanding_pings` (`__init__.py:602`), and force
disconnects again. Every reconnected connection lives at most one
`ping_interval` after a ping-timeout disconnect, forever.

### C3. `CancelledError` swallowed inside the reconnect loop

`__init__.py:1003-1006`:

```python
except (asyncio.CancelledError, TimeoutError) as e:
    logger.error("Failed to connect to %s: %s", server, type(e).__name__)
    self._last_server = server
    continue
```

If `close()` cancels the task while it is inside `establish_connection` for one
server, the cancellation is consumed and the loop *continues* to the next
server. `close()` then blocks on `await self._read_task` until the entire
reconnect schedule (all servers × all attempts × backoff) runs dry.
Cancellation must be re-raised; never `continue` past a `CancelledError`.

### C4. Initial connect treats EOF (and any non-PONG) as success

`__init__.py:1849-1872`: after CONNECT+PING, the response is only checked with
`isinstance(response, Err)`. If the server closes the connection without an
`-ERR` (e.g. some auth/TLS-verify failures), `parse()` returns `None` and
`connect()` happily returns a dead `Client`. The reconnect path
(`__init__.py:945-958`) handles all four cases correctly (`None`, `Err`,
non-`Pong`, timeout) — the initial path should do the same. This verification
logic is duplicated in two places and has already diverged; extract it.

---

## High

### H1. Keepalive PING starves under continuous publish traffic

`_write_loop` (`__init__.py:583-611`) only sends a PING when
`wait_for(self._flush_waker.wait(), timeout=self._ping_interval)` times out.
Any publish sets the waker, so on a connection with steady outgoing traffic the
timeout branch never fires and no PING is ever sent — a dead server (or a
half-open TCP connection) is never detected as long as the app keeps
publishing. Stale-connection detection must be driven by a timer independent of
write activity (check `now - _last_ping_sent >= ping_interval` on every loop
iteration, not only in the timeout branch).

### H2. The 5 ms minimum flush interval serializes request/reply to ~200 req/s

`_DEFAULT_MIN_FLUSH_INTERVAL = 0.005` (`__init__.py:78`) combined with
`_write_loop`'s enforced sleep (`__init__.py:589-592`) means any publish that
lands within 5 ms of the previous flush waits out the remainder. Sequential
`request()` calls each pay this: publish → buffered → flusher sleeps ~5 ms →
flush → response. A local round trip that should take ~100 µs takes ~5 ms,
capping serialized request throughput at roughly 200/s. Reference clients
coalesce at sub-millisecond granularity (flush as soon as the event loop yields,
i.e. coalescing happens naturally via the task scheduling, not a fixed timer).
Consider flushing immediately when the buffer transitions from empty, and using
the interval only as a backpressure coalescing hint. Also note
`_last_flush = current_time` (`__init__.py:596`) stamps the pre-sleep time, so
the interval bookkeeping is wrong by up to the sleep duration.

### H3. Publish buffer grows unboundedly while reconnecting

`publish()` (`__init__.py:1141-1148`) appends to `_pending_messages` during
`RECONNECTING` (status check only blocks `CLOSED`/`CLOSING`), and
`_force_flush()` (`__init__.py:1032-1033`) returns early when the connection is
down without clearing the buffer — there is no flusher task alive either. A
busy publisher during a long outage grows the buffer without limit until OOM.
There needs to be a reconnect-buffer cap (cf. legacy `pending_size`, nats.go
`ReconnectBufSize`, default 8 MB) with a defined overflow behavior (raise or
drop).

### H4. `subscribe()`/`unsubscribe()` are broken while reconnecting

- `subscribe()` (`__init__.py:1193-1210`) registers the subscription in
  `_subscriptions` *before* writing SUB. During `RECONNECTING` the write raises
  `ConnectionError`, the caller sees a failure, but the subscription stays
  registered and gets silently replayed on reconnect — a retry then creates a
  duplicate.
- `Subscription.unsubscribe()` → `_unsubscribe()` (`__init__.py:1230-1241`)
  writes UNSUB for any status other than `CLOSED`/`CLOSING`; during
  `RECONNECTING` the write raises and the `del self._subscriptions[sid]` never
  runs, so the sub is resurrected on reconnect even though the user
  unsubscribed.

Both should tolerate a down connection: mutate local state, skip the wire write,
and rely on the reconnect replay (which already reads `_subscriptions`).

### H5. WebSocket clients poison the reconnect pool with TCP endpoints

`connect()` (`__init__.py:1791-1792`) and `_handle_info`
(`__init__.py:762-765`) append `info.connect_urls` — bare `host:port` TCP
endpoints — into `_server_pool` regardless of transport. For a `ws://`/`wss://`
client, the reconnect loop (`__init__.py:856-871`) then parses those as
`nats://host:port` and attempts raw TCP to the cluster's client ports. The
INFO field `ws_connect_urls` (already declared in `protocol/types.py:85`) exists
precisely for this and is ignored. WebSocket clients should populate the pool
from `ws_connect_urls` only.

### H6. `drain()` does not actually stop publishing (contradicts its own docs)

`drain()`'s docstring promises "No new messages can be published", but
`publish()` (`__init__.py:1095`) only rejects `CLOSED`/`CLOSING`; `DRAINING` and
`DRAINED` sail through (likewise `subscribe()` at `__init__.py:1179` and
`request()` at `__init__.py:1278`). Reference clients raise a
draining-specific error for all three. Additionally,
`asyncio.wait_for(asyncio.gather(*drain_tasks), ...)` (`__init__.py:1356`) is a
no-op timeout: `Subscription.drain()` returns immediately after
`queue.shutdown(immediate=False)` — nothing waits for consumers to work off the
queues, so the timeout protects nothing.

### H7. No header validation → CRLF injection into the header block

`encode_headers` (`protocol/command.py:52-57`) interpolates keys and values
into the wire format with no validation. A value (or key) containing `\r\n`
injects arbitrary header lines; a key containing `:` or whitespace corrupts the
block. Framing stays intact (the block is length-prefixed) so this is not a
command injection like the subject case, but it silently produces malformed or
attacker-shaped headers. Subjects are carefully validated
(`__init__.py:194-226`); headers deserve the same.

---

## Medium

### M1. Concurrent `_force_disconnect` races: status overwrite and double reconnect

`_force_disconnect` (`__init__.py:786-826`) sets
`self._status = DISCONNECTED` *before* taking `_reconnect_lock`. A second caller
(read loop EOF + write loop max-pings can both call) overwrites `RECONNECTING`
with `DISCONNECTED` while the first holds the lock through the *entire*
reconnect cycle. When the first cycle succeeds and releases the lock, the
second caller observes `old_status` not in `(CLOSING, CLOSED)`,
`_reconnecting == False`, and starts a second reconnect cycle on top of the
fresh, healthy connection — replacing `self._connection` and leaking the old
one. The status flip and the should-I-reconnect decision need to happen
atomically, and a freshly reestablished connection must not be torn down by a
stale disconnect notification (guard on connection identity).

### M2. `_force_disconnect` cancels the task it runs on

The read loop calls `_force_disconnect`, which cancels `_read_task` — the
currently running task (`__init__.py:792-795`). The self-`await` raises and the
pending cancellation happens to be consumed by the surrounding
`contextlib.suppress(asyncio.CancelledError, ...)`, after which the reconnect
loop continues on a task whose `cancelling()` count is permanently 1 (no
`uncancel()`). This works by accident of CPython task-step ordering and will
misbehave inside any future `asyncio.timeout()` scope. Restructure so teardown
cancels *the other* task only, or hand reconnection off to a dedicated task.

### M3. Inbound `MAX_CONTROL_LINE` of 4096 can kill valid connections

`protocol/message.py:46,346-348` rejects any server control line over 4096
bytes. 4096 is the *server's default* limit for client→server lines;
`max_control_line` is configurable upward, and a long subject + reply easily
exceeds 4 KiB on a server configured for it. Other clients don't enforce a
fixed inbound cap (or honor the server's advertised one). Also note the check
runs *after* `readline()`, whose own `StreamReader` 64 KiB limit raises a raw
`ValueError`/`LimitOverrunError` first for truly long lines — a noisy break of
the read loop instead of a clean protocol error.

### M4. Slow-consumer handling is inconsistent and leaks `QueueShutDown`

- In `Subscription._enqueue` (`subscription.py:193-207`), the byte-limit check
  raises *before* callbacks run, but `put_nowait` raises `QueueFull` *after*
  callbacks ran — so whether registered callbacks observe a dropped message
  depends on which limit tripped.
- The client catches `(asyncio.QueueFull, ValueError)` (`__init__.py:653,734`)
  but not `asyncio.QueueShutDown`. Today a shut-down subscription is always
  removed from `_subscriptions` first, so it is unreachable in practice — but
  one refactor away from an unhandled exception that kills the read loop.
- Dropped-message accounting uses `len(payload)` while `_enqueue` budgets
  `len(message.data)` — same value, but the duplicated logic in `_handle_msg`
  and `_handle_hmsg` (see U1) makes such drift likely.

### M5. Disconnect/teardown paths produce ERROR-level tracebacks for normal events

A connection dropped mid-payload raises `IncompleteReadError` inside the read
loop's `except Exception` (`__init__.py:543-545`), logging a full traceback via
`logger.exception("Error in read loop")` for an ordinary network event. The
WebSocket transport is worse: EOF in `readline()` propagates
`IncompleteReadError` (`connection.py:280-287`) instead of returning `b""` like
`TcpConnection.readline`, so a clean server close logs as an error instead of
"Connection closed by server". Distinguish expected EOF/reset from genuine
parser/internal errors.

### M6. Dead/misleading exception handling in `_read_loop`

`__init__.py:546-548` catches `(asyncio.CancelledError, ParseError)` at the
outer level and returns *without* reconnecting — but `ParseError` is always
caught first by the inner `except Exception` (which breaks into the reconnect
path). The outer `ParseError` arm is unreachable and documents the wrong
behavior. Similarly, `int()` failures in `parse_msg`/`parse_hmsg`
(`protocol/message.py:187-190,228-233`) escape as raw `ValueError` instead of
`ParseError`.

### M7. `flush()` / `rtt()` misbehave when not connected

- `flush()` during `RECONNECTING`: `_force_flush` no-ops, then `_ping()` writes
  straight to the dead connection and raises `ConnectionError`
  (`__init__.py:1040-1046`). Callers get a transport exception for a state the
  client is supposed to be managing.
- `flush()` on PONG timeout force-disconnects (`__init__.py:1071-1073`) —
  reasonable — but `rtt()` lets the `TimeoutError` escape without any state
  handling, and both share `_pong_waker`, so concurrent `flush()`/`rtt()`
  calls can complete on each other's PONGs (rtt underreports).
- `_ping()` (`__init__.py:1040`) increments `_pings_outstanding` without the
  max-outstanding check that `_queue_ping` has; the two near-identical methods
  should be one.

### M8. Publishing with headers is not gated on server support

`connect()` always sends `headers: true`, and `publish()` happily encodes HPUB
regardless of `server_info.headers` (`__init__.py:1114-1132`). Against an old
or proxied server that advertises `headers: false`, reference clients raise a
clear "headers not supported" error; this client sends a frame the server will
treat as a protocol error and disconnects.

### M9. `ServerInfo.from_protocol` crashes the read loop on minimal INFO

`__init__.py:145-152` indexes `server_id`, `version`, `go`, `host`, `port`,
`headers` directly. Any async INFO (or nonstandard server/proxy) missing one of
these raises `KeyError` inside `_handle_info`, which breaks the read loop and
tears down the connection. Use `.get()` with defaults for everything that isn't
strictly load-bearing.

### M10. Reconnect pool/server-selection nits

- `_last_server` skip (`__init__.py:850-851`) skips the last *attempted*
  server, and `_last_server` is updated on every failed attempt too
  (`__init__.py:1005,1009`) — the net effect is hard to reason about and
  differs from the usual "deprioritize the server we just lost" semantics.
- The IPv6 normalization reassigns the loop variable `server`
  (`__init__.py:861-869`), so `_last_server` stores the bracketed form while
  the pool holds the unbracketed one — the skip comparison never matches for
  IPv6 entries.
- `no_randomize=False` shuffles only the tail, pinning `server_pool[0]`
  (`__init__.py:845-848`); full-pool shuffle is the conventional behavior.
- `reconnect_max_attempts` counts *passes over the whole pool*, not per-server
  attempts, so the effective retry budget scales with cluster size. (The
  `0 == unlimited` sentinel is already tracked separately for a follow-up PR.)
- Reconnect verification reads exactly one protocol message and fails on
  anything that isn't PONG (`__init__.py:955-958`); a server that interleaves
  an async INFO (e.g. entering LDM) before the PONG fails the attempt. Loop
  until PONG/ERR.

### M11. Write-buffer bypass reorders commands

`subscribe()`, `_unsubscribe()`, `_handle_ping`'s PONG, and `_queue_ping` write
directly to the connection while published messages sit in
`_pending_messages`. A SUB can therefore reach the server *before* a PUB that
the application issued earlier — observable with echo enabled (you receive your
own earlier publish) and surprising for anyone reasoning about ordering. Either
route everything through the buffer or flush the buffer before direct writes.

---

## Parity gaps

### P1. `connect()` accepts a single URL only

No way to seed multiple servers (`nats.connect(["nats://a", "nats://b"])` or
`servers=[...]`). The pool only grows via `connect_urls` after the first
connect succeeds — so the bootstrap server is a single point of failure. Every
reference client accepts a server list.

### P2. Auto-unsubscribe is unreachable

`encode_unsub` supports `max_msgs` (`protocol/command.py:105-117`) but nothing
exposes it: `Subscription.unsubscribe()` takes no limit and there is no
`max_msgs=` on `subscribe()`. `UNSUB <sid> <n>` / auto-unsubscribe-after-N is
standard across clients (and required by request-many patterns).

### P3. Callbacks are sync-only and weakly typed

`add_disconnected_callback` et al. accept `Callable[[], None]` only — an
`async def` callback would produce an un-awaited coroutine and a warning.
Legacy nats-py and most asyncio APIs accept coroutine callbacks. Also
`add_error_callback` takes `Callable[[Exception | str], None]`
(`__init__.py:299,1512`): server `-ERR` strings are passed raw while
slow-consumer errors arrive as exceptions. Wrap protocol errors in an exception
type so the callback signature is just `Callable[[Exception], ...]`.

### P4. Missing surface

- No closed/connection-terminal callback (only disconnected/reconnected/error/LDM).
- No `is_connected`-style convenience or `connected_url`/current-server accessor.
- `ClientStatistics` lacks `errors_received` (legacy parity).
- `request()` takes `headers: dict[...]` but not `Headers`
  (`__init__.py:1257`), and `subject: str` but not `bytes` — both inconsistent
  with `publish()`.
- Subscription pending defaults are 65 536 msgs (`__init__.py:1160`) vs the
  conventional 512 × 1024; worth a deliberate decision either way.
- Repeated authorization errors during reconnect retry forever (until
  max attempts); reference clients abort a server after repeated auth
  failures to avoid hammering.
- `new_inbox()` uses `uuid4().hex` (32 chars + prefix) where other clients use
  NUID (22 chars, faster); fine functionally, but inboxes are hot-path.

### P5. Inbox prefix validation misses whitespace/CRLF

`__init__.py:410-417` rejects `>`, `*`, and trailing `.` but not spaces or
CRLF. An injected prefix is only caught later when `publish()` validates the
reply subject — surfacing as a confusing per-request `ValueError` instead of a
clear `connect()`-time error.

---

## Unpythonic / code-quality

### U1. Large-scale duplication

- `_handle_msg` and `_handle_hmsg` (`__init__.py:626-756`) are ~90% identical
  (mux dispatch, slow-consumer handling, error callbacks) — 130 lines that
  should be one helper.
- The reconnect CONNECT/verify block (`__init__.py:896-958`) duplicates
  `connect()`'s (`__init__.py:1794-1872`), and the two have already diverged
  (C4). Extract a shared "send CONNECT, await PONG" helper.
- `subscribe()` inlines the body of `_subscribe()` (`__init__.py:1204-1210` vs
  `1214-1228`).
- `close()` cancels `_read_task`/`_write_task` twice, ~30 lines apart
  (`__init__.py:1419-1427` and `1451-1462`).
- `_queue_ping` vs `_ping` (M7).

### U2. `assert` used for runtime control flow

`__init__.py:632,692` (`assert self._request_prefix is not None` — an
unexpected sid-0 message under `python -O` becomes an `AttributeError`/wrong
behavior, and under normal mode an `AssertionError` that kills the read loop)
and `_setup_jwt_auth`'s `assert isinstance(...)` (`__init__.py:1650-1651`,
should raise `TypeError`). Asserts vanish under `-O`; user-input validation
must not rely on them.

### U3. `Headers` is a fake dataclass and not a Mapping

`message.py:8-26` decorates `Headers` with `@dataclass` but hand-writes
`__init__` and `__eq__`, so the decorator generates nothing useful (and the
custom `__eq__` silently sets `__hash__ = None`). More importantly it supports
neither `headers["Key"]`, `"Key" in headers`, `len(headers)`, nor iteration —
`collections.abc.Mapping` is the obvious shape (`get`, `items`, `keys`,
`values`, `__contains__` for free). Also `asdict()` returns a shallow copy
whose lists are shared with internal state — mutating
`headers.asdict()["X"].append(...)` mutates the live headers.

### U4. Event-loop API inconsistency

`asyncio.get_event_loop().time()` in `__init__.py:452,457,560,576,589,599` vs
`asyncio.get_running_loop()` at `__init__.py:1045,1053`. Inside coroutines the
modern form is `get_running_loop()` everywhere; `get_event_loop()` is
soft-deprecated and slower.

### U5. `Subscription.messages` swallows `RuntimeError` to end iteration

`subscription.py:123-130` breaks the async iterator on *any* `RuntimeError`,
not just "subscription closed". A genuine `RuntimeError` from user code or
asyncio internals silently terminates the message stream. Define a dedicated
`SubscriptionClosedError` (or re-raise unless the subscription is actually
closed). The `next()`-raises-`RuntimeError` contract has the same smell.

### U6. Protocol-module oddities

- `ping()`/`pong()`/`parse_info()`/`parse_err()` are `async def` with no
  awaits (`protocol/message.py:258-325`); `Ping`/`Pong` could be module-level
  singletons returned synchronously.
- `if TYPE_CHECKING: pass` dead block (`protocol/message.py:10-11`).
- `parse` accepts bare `b"ERR"` in addition to `b"-ERR"`
  (`protocol/message.py:365`) — the server never sends the former.
- Control-line split is single-space only (`protocol/message.py:350`);
  consecutive spaces/tabs (which the Go parser tolerates) produce empty args
  and raw `ValueError`s.
- `ConnectInfo` marks every default field `Required[...]` in a `total=True`
  TypedDict (`protocol/types.py:14-22`) — redundant noise.

### U7. Exception-chaining and transport nits

- `open_tcp_connection` raises `ConnectionError(msg)` without `from e`
  (`connection.py:209-211`); the websocket twin chains correctly.
- `TcpConnection.upgrade_to_tls` pokes `self._writer._transport`
  (`connection.py:118`) — known asyncio wart, but deserves a comment on why
  it's safe w.r.t. `drain()` and the reader protocol.
- `open_websocket_connection` passes `max_size=None` (`connection.py:328`) —
  unbounded frame buffering from the server; the parser's own caps never get a
  chance to apply.
- `TcpConnection.read()` (and `Connection.read` in the protocol) is dead code —
  nothing in the client calls byte-granularity `read`.

### U8. Docstring defects

- `Client.server_info` is annotated `-> ServerInfo | None`
  (`__init__.py:472-475`) but `_server_info` is always set; the `| None` forces
  every caller to narrow for no reason.
- `connect()`'s docstring references type aliases named `Nkey` and `JWT`
  (`__init__.py:1720-1724`) — the actual names are `NkeySeed`/`NkeyHandlers`
  and `JWTCredentials`/`JWTHandlers`.
- `_validate_subject`, `_validate_queue`, and the `skip_subject_validation`
  docs cite nats.go/nats.rs behavior by name
  (`__init__.py:202-206,232-234,1729-1730`); per project convention, describe
  the behavior directly instead of citing other clients.
- The type aliases (`NkeySeed`, `JWTCredentials`, …) are public-facing but
  absent from `__all__` (`__init__.py:1909-1924`).

---

## Summary

| Severity | Count | Headliners |
|----------|-------|------------|
| Critical | 4 | broken bare install (C1), reconnect ping death spiral (C2), swallowed cancellation (C3), EOF-as-success on connect (C4) |
| High | 7 | keepalive starvation (H1), 5 ms request serialization (H2), unbounded reconnect buffer (H3), sub/unsub during reconnect (H4), ws pool poisoning (H5), drain doesn't block publish (H6), header injection (H7) |
| Medium | 11 | disconnect races (M1/M2), inbound control-line cap (M3), slow-consumer inconsistencies (M4), noisy/dead error paths (M5/M6) |
| Parity | 5 | single-URL connect (P1), no auto-unsubscribe (P2), sync-only callbacks (P3) |
| Quality | 8 | duplication (U1), asserts as control flow (U2), fake-dataclass Headers (U3) |

The architecture (Protocol-typed transports, NamedTuple wire messages,
`match`-based dispatch, Queue.shutdown-driven subscription lifecycle) is sound
and idiomatic for 3.13. The risk is concentrated in connection lifecycle code:
`_force_disconnect`/reconnect is one 240-line function owning cancellation,
locking, pool management, CONNECT/auth replay, and state transitions — C2, C3,
C4, M1, M2, M6, and M10 all live there. Decomposing it (dedicated reconnect
task, shared CONNECT/verify helper, atomic state transitions) would resolve the
bulk of this audit in one structural change.
