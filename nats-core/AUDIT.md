# nats-core audit (2026-05-27)

Eight specialized agents reviewed `nats-core/src/nats/client/` through different
lenses (code smells, pythonic patterns, ADR compliance, nats.go divergence,
security, asyncio/concurrency, protocol parser, public API). This document is
the deduplicated, severity-ranked synthesis. Items confirmed by multiple lenses
are noted in parentheses.

## Critical — fix first

1. **TLS downgrade on reconnect leaks credentials** *(security, ADR-40)*
   `__init__.py:872-896, 936, 1844`. On reconnect, TLS upgrade is gated on the
   *current* INFO advertising `tls_required`/`tls_available`. A MITM stripping
   those flags causes CONNECT — with `auth_token`/`password`/`jwt`/`nkey`/`sig`
   — to be written in plaintext. Pin TLS intent at first connect; never consult
   server INFO to downgrade.

2. **Server-driven `connect_urls` is unbounded and unvalidated** *(security; ADR-40 defines the mechanism, not the hardening)*
   `__init__.py:698-701, 968-971`. A hostile/compromised server can append
   arbitrary hosts to the client's reconnect pool; entries are never pruned, no
   allowlist, TLS hostname is not re-pinned for discovered URLs. Combined with
   #1 this is a credential-stealing chain. ADR-40 specifies that advertised
   URLs are stored and used for reconnect, with discovery **on by default** and
   only a boolean opt-*out* ("Ignore advertised servers", default false); it
   says nothing about pruning, allowlisting, bounding the pool, or re-pinning
   TLS, and its Security Considerations section is an empty stub. So the fix
   here is hardening beyond the spec, not ADR conformance: keep ADR-40's
   opt-out default (do **not** flip discovery to opt-in — that would diverge
   from the spec and every other client), prune on each INFO, and require TLS
   for inherited servers when the seed was TLS.

3. ~~**Concurrent writes to the connection are unsynchronized** *(asyncio)*~~
   **Retracted.** Every call site passes a complete frame (`encode_pub`/
   `encode_ping`/`encode_sub` all return one `bytes`; the buffered path
   `b"".join(...)` first). `Connection.write` is `self._writer.write(data);
   await self._writer.drain()` — the `writer.write` call is a single C-level
   append, atomic under the GIL, and `drain()` flushes in FIFO append order.
   Two coroutines calling `connection.write(complete_frame)` land their frames
   adjacent on the wire, not interleaved. Would be a real hazard if a frame
   were split across two writes with an await between — it isn't.

4. **`_force_disconnect` invoked from the read task awaits itself** *(asyncio)*
   `__init__.py:728-736`. The `RuntimeError("Task cannot await on itself")` is
   silently swallowed by `contextlib.suppress`. Disconnect/reconnect proceeds
   before the old reader actually terminates. Detect
   `asyncio.current_task() is self._read_task` and skip the await, or move
   reconnect to a supervisor task.

5. **Reader loop silently disconnects on `ParseError`** *(asyncio, parser)*
   `__init__.py:482`. `except (CancelledError, ParseError): return` skips
   `_force_disconnect`, leaving the client in `CONNECTED` with a dead socket.
   Re-raise `CancelledError`; route `ParseError` through reconnect.

## High

6. **Credentials logged at DEBUG** *(security)* — [PR #955](https://github.com/nats-io/nats.py/pull/955)
   `__init__.py:935, 1844`. `logger.debug("->> CONNECT %s", json.dumps(connect_info))`
   dumps `auth_token`/`password`/`jwt`/`nkey`/`sig` in cleartext. Redact before
   logging.

7. **CRLF / whitespace injection in subject, reply, queue, headers** *(security, parser)*
   `__init__.py:1078-1205`, `protocol/command.py:28-117, 52-57`. Encoders
   interpolate caller-supplied bytes verbatim with `b"PUB %b ..."` /
   `f"SUB {subject} {sid}\r\n"`. A `\r\n` in a subject or header value forges
   arbitrary protocol commands. Validate against the subject grammar; reject
   CR/LF in header keys/values. Inbox-prefix validation at `__init__.py:347-356`
   also misses CR/LF/whitespace.

8. **No typed `-ERR` model — Stale-Connection, Auth, Permissions invisible** *(parser, nats.go, ADR-7)* — [PR #956](https://github.com/nats-io/nats.py/pull/956)
   `protocol/message.py:304-307`, `errors.py`, `__init__.py:711-720`. `-ERR`
   arrives as an opaque string; only `MaxPayloadError` exists locally and only
   for client-side checks. Callers can't programmatically react to auth failure
   vs transient error vs reconnect-required error. (Abort-reconnect-on-auth is
   a follow-up now unblocked.)

9. **Sequential request tokens within a session are predictable** *(security, nats.go)*
   `__init__.py:1283`. `_next_request_id` is a monotonic int under a per-client
   UUID prefix. A subscriber to `_INBOX.<hex>.*` (e.g. a co-tenant in the same
   account) can reply-spoof other requests. Use `secrets.token_hex(8)` per
   request — nats.go uses NUIDs per request, not sequential ints.

10. **`_force_flush` clear-after-await drops and double-sends concurrently-buffered messages** *(asyncio)*
    `__init__.py:1090-1101, 1204-1211`. Not a data race — the GIL and the
    single event loop already serialize bytecode, and the limit-check →
    `append`/`+=` tail of `publish()` is await-free, so two callers cannot both
    observe under-limit and overrun `_max_pending_bytes`. The hazard is
    cooperative interleaving across the one suspension point, `await
    self._connection.write(b"".join(self._pending_messages))` in
    `_force_flush`, and only with concurrent publishers (e.g. `gather`, or
    publishing from multiple tasks). The `join` snapshots the buffer, then
    `drain()` yields; another `publish()` can `append` into the same list
    before the resuming flush calls `clear()`, so that message is silently
    dropped (it was never in the snapshot). Worse, two over-limit publishers
    can both pass the `if not self._pending_messages` guard and `write` the
    same buffer before either clears, putting those messages on the wire twice.
    Swap in a fresh list *before* the await (`batch, self._pending_messages =
    self._pending_messages, []; self._pending_bytes = 0; await ...write(b"".join(batch))`),
    which closes both the drop and the double-send.

11. **HMSG/MSG numeric parsing crashes → silent disconnect** *(parser)*
    `protocol/message.py:185-194, 226-241`. `int(args[2])` on corrupt/attacker
    input raises, the read loop's blanket `except Exception` catches it and
    `break`s. Same pattern for non-UTF-8 bytes in headers (`message.py:124`) —
    strict `.decode()` plus the silent break means one bad frame disconnects
    the client without a typed error.

12. **`+OK` is not parsed at all** *(parser)* — [PR #949](https://github.com/nats-io/nats.py/pull/949)
    `protocol/message.py:354-369` has no `b"+OK"` arm. Any user passing
    `verbose=True` in a custom CONNECT silently disconnects on the first server
    reply.

13. **Treating any non-200 status as error breaks 1xx informational responses** *(API, parser)*
    `__init__.py:1295`, `errors.py:71`. `request()` raises `StatusError` on
    `100 Idle Heartbeat` / `100 Flow Control`. Status code is also a `str`,
    not `int`.

14. **`Client` is a god-class — 1923 lines in `__init__.py`, ~40 attrs, duplicated handshake/CONNECT/TLS logic** *(smells)* — [PR #948](https://github.com/nats-io/nats.py/pull/948) extracts `establish_connection`, killing ~150 lines of the open-socket + read-INFO + maybe-upgrade-TLS dedup. Remaining work: `_build_connect_info` + `_perform_handshake`, then `RequestMultiplexer` / `WriteBuffer` / `ServerPool` extraction.

15. **`Headers` is not a `Mapping`, keys are case-sensitive** *(API, parser, spec)* — [PR #954](https://github.com/nats-io/nats.py/pull/954)
    `message.py:8-109`. Cannot do `msg.headers["trace-id"]`, `in`, `len()`,
    `iter()`, `dict(headers)`. ADR-21/HTTP-style is case-insensitive; nats.go
    canonicalizes. Inherit `collections.abc.MutableMapping`, store case-preserved
    but case-insensitive lookup.

16. **No `Message.respond()`** *(API)* — [PR #953](https://github.com/nats-io/nats.py/pull/953)
    Every reply forces `await client.publish(msg.reply, ...)` with manual
    `msg.reply is not None` guards. This is the single most-used convenience in
    NATS — add it.

## Medium

17. **No offline publish buffer / no PUB replay on reconnect** *(nats.go)* —
    `publish` during `RECONNECTING` raises `RuntimeError`; nats.go buffers up to
    8 MB and replays.
18. **Reconnect backoff/jitter semantics diverge from nats.go** *(nats.go)* —
    multiplicative jitter + exponential base doubling; Go uses additive
    `ReconnectJitter` (100 ms / 1 s TLS) with no exponential. Missing
    `ReconnectJitterTLS`, `CustomReconnectDelayCB`, `RetryOnFailedConnect`,
    `IgnoreAuthErrorAbort`, `ReconnectBufSize`, `ConnectedCB`/`ClosedCB`,
    `DiscoveredServersCB`, `ReconnectErrCB`, `CustomDialer`, `RootCAsCB`.
19. **Per-server reconnect counter / dead-server eviction missing** *(nats.go)* — [PR #960](https://github.com/nats-io/nats.py/pull/960). Every server was retried forever; now `reconnect_max_attempts` is per-server and servers are evicted on exhaustion (behavioral change).
20. **ADR-5 lame-duck mode** *(ADR)* — detected and callback fires, but no
    proactive jittered self-disconnect/migration.
21. **ADR-11 multi-IP hostname fallback** *(ADR)* — `asyncio.open_connection`
    picks one address; no per-IP retry or randomization.
22. ~~**`_next_sid` and `_next_request_id` non-atomic increment** *(asyncio)*~~ —
    **Retracted.** `sid = self._next_sid; self._next_sid += 1` is pure Python
    bytecode with no `await` between read and increment. The asyncio loop is
    single-threaded and only context-switches at `await`, so the sequence is
    atomic for our purposes. Would be a real race in a multi-threaded context;
    we're not in one.
23. **`flush()`/`rtt()` share a single `_pong_waker`** *(asyncio)* — keepalive
    PONG can wake `flush()` early; two concurrent flushes both wake on a single
    PONG. Use a deque of per-call futures.
24. **`_force_disconnect` not idempotent under concurrent callers** *(asyncio)* —
    lock only covers the post-cancel block; `close()` racing with reader-
    triggered disconnect double-runs.
25. **`subscribe`/`_unsubscribe` bypass the publish buffer** *(smells)* —
    head-of-line ordering surprise between buffered PUB and unbuffered SUB to
    the same subject.
26. **No `unsubscribe(max_msgs=...)`** *(API, nats.go)* — protocol supports it
    (`encode_unsub` even takes the arg), but no API surface.
27. **No shared exception base class** *(API)* — MIGRATION.md admits this. Bare
    `RuntimeError("Connection is closed")` in `publish`/`subscribe`/`request`.
    Add `NATSError`, `ConnectionClosedError`.
28. **In-flight `_request_futures` are not failed on disconnect** *(asyncio, smells)* —
    callers see `TimeoutError` instead of a connection error.
29. **`Subscription._enqueue` raises mixed exceptions** *(smells)* — `ValueError`
    for bytes overrun, `QueueFull` for message overrun; callers branch identically.
30. **Pending limits immutable after `subscribe()`** *(nats.go)* — no
    `set_pending_limits` on `Subscription`.
31. **ADR-4 header field-name validation missing** *(ADR)* — invalid keys reach
    the wire and cause server disconnects.
32. **User-supplied `nkey_signature_handler` bytes are not base64url-encoded** *(ADR-14)* —
    `_setup_nkey_auth` correctly encodes when the client signs, but the
    `NkeyHandlers` path forwards `.decode()` raw bytes. Document or wrap.
33. **`add_*_callback` methods are Java-flavored**; no `remove_disconnected_callback` *(API)* — [PR #957](https://github.com/nats-io/nats.py/pull/957) adds the four `remove_*_callback` counterparts (`add_*_callback`/`remove_*_callback` matches stdlib `Future.add_done_callback`/`remove_done_callback`, so the "Java-flavored" criticism was overstated).
34. **Reconnect cannot be aborted by `close()` mid-backoff** *(asyncio)* — close
    should also set `_reconnect_wake`.
35. **Subscription dict mutated during reconnect's re-SUB iteration** *(asyncio)* —
    user `subscribe`/`unsubscribe` during the window races with the bulk re-SUB
    write.
36. **`close()` cancels read/write tasks twice and writes UNSUB after the socket is closed** *(asyncio, smells)*.
37. **`_handle_msg` / `_handle_hmsg` are near-identical** *(smells)* — collapse
    to one `_dispatch`.
38. **Inbox uses UUID4 (32 hex) instead of NUID (22 chars)** *(nats.go)* —
    interop and forensic-tooling deviation.
39. **`ServerInfo` drops `ws_connect_urls`, `git_commit`, `ip`, `client_ip`, `cluster`, `domain`, `xkey`** *(parser, ADR)*.
40. **IPv6 detection by colon-counting** *(smells)* — `__init__.py:797-806`; use
    `ipaddress` or require brackets.

## Low / polish

41. `from __future__ import annotations` in 8 files — dead weight at 3.13 *(pythonic)*.
42. `asyncio.get_event_loop()` at `__init__.py:388, 393, 496, 512, 525, 535`
    while line 1048 correctly uses `get_running_loop()` *(pythonic, asyncio)*.
43. `asyncio.TimeoutError` mixed with `TimeoutError` builtin *(pythonic)* — [PR #950](https://github.com/nats-io/nats.py/pull/950).
44. `@dataclass` missing `slots=True` on `ServerInfo`; `Headers` is `@dataclass`
    *and* defines `__init__`/`__eq__` (decorator is dead) *(pythonic, API)*.
45. `Enum` instead of `StrEnum` for `ClientStatus`; eight states with
    undocumented transitions *(pythonic, API)*.
46. Reader/writer broad `except Exception` and `except BaseException` swallows
    real bugs and `CancelledError` *(pythonic, asyncio)*.
47. WebSocket buffer is O(n²) `bytes += frame` / slicing *(pythonic)*.
48. `protocol/message.py:310-325` `async def ping/pong` with no awaits;
    `if TYPE_CHECKING: pass` dead block; unused `TypeVar T` in
    `subscription.py:22`.
49. `connection.py:114` pokes `_writer._transport` private attr (justified,
    document it).
50. `force_reconnect()` is Go/Java-named — prefer `reconnect()` *(API)*.
51. `return_on_error` is a double-negative kwarg — prefer `raise_for_status` *(API)*.
52. Magic numbers `1*1024*1024`, `1*512`, `0.005` *(smells)* — [PR #951](https://github.com/nats-io/nats.py/pull/951).
53. Verb match is case-sensitive (`PING` vs `Ping`) — spec is case-insensitive *(parser)*.
54. `__all__` exports `MaxPayloadError`/`NoRespondersError` but not
    `SlowConsumerError` *(API)*.
55. `Subscription.messages` exists "for legacy compat" — drop it *(API)*.
56. INFO `cast(ServerInfo, data)` skips required-field validation *(parser)*.
57. `Verbose`/`Pedantic` hard-coded in CONNECT — no user override *(nats.go)* — [PR #949](https://github.com/nats-io/nats.py/pull/949). `protocol` deliberately left as a wire-format internal.
58. Inconsistent `logger.exception` vs `logger.error` for similar failure paths *(smells)* — [PR #952](https://github.com/nats-io/nats.py/pull/952).

## Themes / leverage points

- **Decomposing `Client` (#14) unlocks unit testability** and removes the
  duplicate CONNECT/TLS branches that drove findings #1, #6, the asyncio races,
  and several smells.
- **A typed `-ERR` model + a header-spec-compliant `Headers`/`Status`** (#8, #13,
  #15) is one refactor that resolves three lenses simultaneously and aligns
  with both ADRs and nats.go.
- **Security #1 + #2 (TLS pinning + `connect_urls` validation) are the only
  items where a remote attacker can cause direct harm** — these should ship
  before the package leaves "in development."
- ~~**One write lock around `_connection.write()`** (#3) is a ~10-line fix with
  very high payoff against a real wire-corruption hazard.~~ Retracted — see #3.
