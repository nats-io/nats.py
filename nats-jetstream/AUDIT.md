# nats-jetstream audit (2026-05-27)

Five specialized agents reviewed `nats-jetstream/src/nats/jetstream/` through
different lenses (ADR compliance, nats.go parity, pythonic patterns, code
smells, public API ergonomics). This document is the deduplicated,
severity-ranked synthesis. Items confirmed by multiple lenses are noted in
parentheses.

Package is ~7.4k LOC across 11 files. Notable concentrations: `api/types.py`
(1774), `stream.py` (1673), `__init__.py` (911), `consumer/pull.py` (862).

## Critical — correctness / interop bugs

1. **Hard-coded `$JS.API.DIRECT.GET.{name}` ignores prefix/domain** *(smells, ADR-31)* — [PR #958](https://github.com/nats-io/nats.py/pull/958)
   `stream.py:1174, 1179`. Bypasses `self._prefix`. Direct-get fails for users
   with a custom prefix or JetStream domain.

2. **`OrderedConsumer.fetch()` recreates the server-side consumer on every call** *(API, ADR-17)*
   `consumer/ordered.py:178-213`. `_prepare_fetch` triggers a full delete + create cycle
   between batches. Either make ordered consumers `messages()`-only or make
   `fetch()` transparent. The docstring acknowledges the bug.

3. **`PullMessageBatch`/`PullMessageStream` leak callbacks on the client** *(smells)*
   `consumer/pull.py:84-86, 282-283`. Each `fetch()` / `messages()` calls
   `add_disconnected_callback` + `add_reconnected_callback` and never removes
   them. Cumulative leak per invocation. Cleanup must `remove_*_callback`.

4. **Pull `messages()` mishandles `Nats-Pending-Messages`/`Nats-Pending-Bytes` on 404** *(parity)*
   `consumer/pull.py:335-341`. On 404 No Messages, the spec says the request
   still counted against the batch — client decrements pending by the headers.
   nats-jetstream zeros out pending unconditionally instead of reading the
   headers, causing drift from the server's view on intermittent gaps.

5. **`_cleanup` in `__anext__` converts errors to `StopAsyncIteration`** *(smells, parity)*
   `consumer/pull.py:322`. Callers can't distinguish connection loss from
   end-of-batch. `PullMessageBatch` stashes on `self._error`; `PullMessageStream`
   doesn't.

6. ~~**`AckPolicy = Literal["none", "all", "explicit", "flow_control"]`** *(pythonic, ADR)*~~
   **Retracted.** `"flow_control"` is a real server policy
   (`AckFlowControl` in `nats-server/server/consumer.go` — "functions like
   AckAll, but acks based on responses to flow control"). The server requires
   it to be paired with a push consumer (`deliver_subject` set) and
   `flow_control=true`, so combined with nats-jetstream being pull-only today
   it can't actually be used end-to-end — but the literal value is correct,
   not hallucinated. (See related finding on push-only fields exposed on a
   pull-only package.)

## High

7. **Pinned-client priority groups silently broken** *(ADR-42)*
   `consumer/pull.py:330-369`. No `Nats-Pin-Id` capture, no `id` echo in
   subsequent pulls, no 423 handling. Config fields (`group`/`priority`/
   `min_pending`/`min_ack_pending`/`priority_policy`) are wired but the
   protocol completion is missing; `CONSUMER.UNPIN` admin API absent.

8. **`publish` lacks first-class JetStream options** *(API, parity, ADR-37)*
   `__init__.py:281-359`. No `msg_id`, `expected_stream`, `expected_last_seq`,
   `expected_last_subject_seq`, `expected_last_msg_id`, `ttl`. Users hand-craft
   `Nats-Msg-Id` / `Nats-Expected-*` / `Nats-TTL` / `Nats-Marker-Reason`
   headers. Header-name constants for these missing in `headers.py`.

9. **No `publish_async` / batched publish** *(parity, ADR-50)*
   Only synchronous one-at-a-time publish. nats.go has `PublishAsync` →
   `PubAckFuture` with max-pending window. ADR-50 batch headers
   (`Nats-Batch-Sequence`, `Nats-Batch-Commit`) and `PublishAck.batch_id`/
   `batch_size` exist (`headers.py:7-23`, `__init__.py:215-228`) but no
   orchestration that drives them.

10. **No `create_or_update_stream`** *(API, parity)*
    `__init__.py:436`. Asymmetric with `create_or_update_consumer`
    (`__init__.py:618`). Single most common operation in real apps.

11. **`update_stream` returns `StreamInfo`, not `Stream`** *(API)*
    `__init__.py:465-483`. Asymmetric with `create_stream → Stream`. Callers
    lose the handle.

12. **Direct Get is single-message only** *(ADR-31, parity)*
    `stream.py:1168-1230`. Missing batch / `max_bytes` / `multi_last` /
    `up_to_seq` / `up_to_time` / `next_by_subj` / `start_time`, EOB-204
    handling, `Nats-Num-Pending` / `Nats-Last-Sequence` propagation, 413
    handling. Reduces a 2.11+ feature to its 2.10 capability set.

13. **Ordered-consumer invariants not enforced** *(ADR-17, parity)*
    `consumer/ordered.py:329-360`. Spec requires `ack_policy=none`,
    `max_deliver=1`, `mem_storage=true`, `num_replicas=1`,
    `flow_control=true`, default `idle_heartbeat≈5s`. Package hard-codes some,
    silently overrides others (`inactive_threshold` → 5min if `None`,
    `consumer/ordered.py:327`), and ignores user values that ever might be
    exposed. Recovery is coarse (any inner-iter exit triggers reset) — no
    explicit sequence-gap-vs-heartbeat distinction.

14. **`Stream` is a god-class with 11 private-attr workarounds** *(smells)*
    `stream.py:1085-1673` — ~590 LOC, 19 public methods. 11 sites do
    `getattr(self._jetstream, "_api", None)` then `raise RuntimeError("can't
    happen")` (`stream.py:1135, 1164, 1317, 1332, 1350, 1403, 1435, 1481,
    1577, 1595, 1635`). `JetStream._api` is always set; the guards exist only
    to defeat typing. Make `_api` a real attribute on a typed protocol.

15. **Pull consumer polls every 100 ms instead of refilling on threshold** *(parity, smells)*
    `consumer/pull.py:426-448` (request loop), `:450-473` (heartbeat monitor).
    Three places independently track `_heartbeat_deadline`; the monitor
    rewrites `_pending_messages`/`_pending_bytes` without coordinating with
    in-flight `__anext__` — real race. Replace with `asyncio.Event` /
    `wait_for` and own the deadline in one place.

16. **Sparse typed error catalog** *(parity, API)*
    `errors.py:7-41`. Missing common codes: `BAD_REQUEST (10003)`,
    `STREAM_WRONG_LAST_SEQUENCE (10071)`, `CONSUMER_NAME_EXISTS (10013)`,
    `CONSUMER_ALREADY_EXISTS (10105)`, `DUPLICATE_FILTER_SUBJECTS (10136)`,
    `OVERLAPPING_FILTER_SUBJECTS (10138)`, `CONSUMER_EMPTY_FILTER (10139)`.
    No `BadRequestError`, `WrongLastSequenceError`,
    `ConsumerNameAlreadyExistsError`. `ErrorCode` is a bare class — should be
    `IntEnum`.

17. ~~**`api/types.py` 1774 LOC with massive duplication** *(smells, pythonic)*~~
    **Retracted.** `api/types.py` is generated from JSON schemas by
    `nats-jetstream/tools/generate_types.py` (schemas under
    `nats-jetstream/schemas/jetstream/api/v1/`). The "3 edits per field change"
    cost is paid by the schema source, not by us. The remaining valid concern
    is #18 — the hand-rolled `from_response`/`to_request` layer on the
    user-facing dataclasses that re-encodes the generated TypedDicts.

18. **~25 `@dataclass` types with hand-rolled `from_response`/`to_request`** *(smells, pythonic)*
    `ConsumerConfig.from_response` ~100 lines; `StreamConfig` `from_response` +
    `to_request` ~220 lines. Per project memory, msgspec was meant to back
    these. Adding a field is 6 edits and silent on omission.

19. **Pull batch 408/409 errors silently swallowed** *(parity, smells)*
    `consumer/pull.py:151-152, 175-186`. 408 raises `StopAsyncIteration`
    without setting `_error`; "exceeded maxrequestbatch/expires/maxbytes/
    maxwaiting" matched by `description.lower()` substring (server text isn't
    API-stable) and converted to bare `Exception`. Use error codes; surface as
    typed errors.

20. **`publish` retry loop control-flow bug** *(API, smells)*
    `__init__.py:323-359`. The `for` loop returns inside the `try`; a future
    edit adding a new exception type could fall through and return `None`.
    Add an explicit `raise` after the loop.

21. **JetStream not `async with`-able, no `close()`** *(API)*
    `__init__.py:248-264`. No `__aenter__`/`__aexit__`, no graceful shutdown.

22. **`JetStream.get_message` / `get_last_message_for_subject` claim to require `allow_direct=true` but use the API path** *(smells)*
    `__init__.py:763-849`. Docstring is wrong and the implementation duplicates
    `Stream._get_message`'s non-direct branch (~90 lines). Plus there's a
    third copy of the base64+headers decode in the same package.

## Medium

23. **`Consumer.reset` on the protocol; `OrderedConsumer.reset` raises `NotImplementedError`** *(API)* — LSP violation. Split into `ResettableConsumer`, or move off the protocol.
24. **`Consumer` protocol mismatch with `PullConsumer.next`** *(API)* — protocol declares `(max_wait)`; impl adds `heartbeat`, `min_ack_pending`, `min_pending`, `priority_group`, `priority`. Protocol is a lie. `consumer/__init__.py:553` vs `consumer/pull.py:566`.
25. **Per-message TTL: config wired, headers/publish path missing** *(ADR-37)* — `StreamConfig.allow_msg_ttl` exists; no `Nats-TTL` constant or `publish(ttl=...)` kwarg.
26. **`Stream.pause_consumer(pause_until: float)` takes Unix timestamp** *(API)* — should be `datetime` to match `ConsumerConfig.pause_until: datetime`. `stream.py:1563`.
27. **`PullMessageStream` rejects `max_messages + max_bytes`** *(parity)* — Go allows both; `max_bytes` is a soft cap within the batch. `consumer/pull.py:644-646`.
28. **No `StopAfter` option on `messages()`** *(parity)*.
29. **No `ConsumeErrHandler` callback** *(parity)* — non-terminal errors only logged, no user hook. `consumer/pull.py:119, 458`.
30. **Fixed 5-second timeout on every API call** *(parity)* — `api/client.py:424`. No per-call override; `stream_create` on large mirror sources / `stream_purge` on huge streams will time out.
31. **`api/client.py:155-395` repeats the same try/`error_code`/raise-subclass pattern ~10 times** *(smells)* — a `{ErrorCode.X: ErrorXError, ...}` map + `_remap(e, allowed)` helper eliminates it.
32. **`pause_consumer` returns `None`; nats.go returns `ConsumerPauseResponse`** *(parity)* — loses the actual pause time.
33. **`Message.ack_sync` missing; called `double_ack` instead** *(parity, smells)* — uses `subscription.next` instead of `request`, opening a sub per call (extra round-trip). And five near-identical ack methods (`ack`/`nak`/`nak_with_delay`/`in_progress`/`term`/`term_with_reason`) repeat the same 4-line `_reply`/`_jetstream` validation; extract `_send_ack(payload)`.
34. **`Message.metadata` always populated, with junk defaults** *(API)* — server-pushed messages without a JS reply silently get `stream=""`, `sequence=(0,0)`. Should be `None` or raise.
35. **`ConsumerConfig` exposes push-only fields on a pull-only package** *(API)* — `deliver_subject`, `deliver_group`, `flow_control`, `idle_heartbeat`, `direct` ("internal use"); `stream.py:1376` rejects push but no validation upfront.
36. **Auto-generated consumer name is `consumer-{base64(str(datetime.now(utc)))}`** *(smells)* — magic, not collision-proof, bizarre. Use `uuid.uuid4().hex` or NUID. `stream.py:1338-1380`.
37. **`StreamConfig.name: str | None = None`; `create_stream` validates at runtime** *(API)* — make `name` required positionally, or take it as the primary arg.
38. **Magic `$JS.API` literal in `stream.py:1174, 1179`** *(smells)* — hard-coded direct-get subject ignores `self._prefix` (see #1).
39. **`StreamConfig` missing `metadata` field on the public dataclass** *(ADR-33)* — present in `api/types.py` but not exposed on the user-facing model. Affects ADR-44 versioning too (`NATS_REQUIRED_API_LEVEL` constant exists; nothing sets/reads it).
40. **`MessageBatch.error` is `Exception`, not typed** *(API, parity)* — 409 sub-errors matched by string substring (#19).
41. **Cleanup / `_delete_consumer` silently swallows all exceptions** *(smells, ordered)* — `consumer/ordered.py:407-412`. No `logger.debug`. Combined with fire-and-forget `_reset()` task creation, recreation failures are invisible.
42. **`getattr(self._jetstream, "_api", None)` 11 times** *(smells)* — see #14.
43. **`StreamNameBySubject`, `UnpinConsumer`, push consumer surface, `CleanupPublisher`, `Stream.cached_info()` missing** *(parity)*.
44. **Pull/`messages()` `max_messages` semantics differs from `fetch()`** *(API)* — same parameter name, different meaning (per-batch hint vs total bound). `consumer/pull.py:566-602` vs `:610-670`.
45. **`Stream.get_message` vs `Stream.direct_get_message`** *(API)* — `Stream._get_message` does fallback transparently; `JetStream.get_message` doesn't. Two routes diverge.
46. **`stream_names`/`list_streams`/`consumer_names`/`list_consumers` pagination duplicated 4 times** *(smells)* — extract `_paginate`.
47. **All dataclasses lack `frozen=True`/`slots=True`/`kw_only=True`** *(pythonic)* — conceptually immutable returned-value records (`PublishAck`, `APIStats`, `Tier`, `AccountInfo`, `Metadata`, `StreamMessage`, `ConsumerReset`, etc.). Either dataclass with all three, or msgspec.
48. **`time.time()` used everywhere for deadlines** *(pythonic)* — `consumer/pull.py:76, 78, 91, 97, 118, 130, 142, 277, 288, 294, 328, 457, 468`. Must be `time.monotonic()` (wall-clock-jump immune).
49. **`asyncio.get_event_loop().time()` for publish deadlines** *(pythonic)* — `__init__.py:319, 326, 353`. Deprecated when no loop; use `get_running_loop()` or `time.monotonic()`.
50. **Manual polling in `_request_loop` / `_heartbeat_monitor`** *(pythonic)* — see #15.
51. **`datetime.fromisoformat(s.replace("Z", "+00:00"))` repeated 11 times** *(pythonic)* — Python 3.11+ handles `Z` natively. Drop the `.replace`.
52. **`timedelta(microseconds=ns / 1000)` repeated 9 times** *(pythonic, bug-shape)* — float divide introduces rounding; wrap as `_ns_to_timedelta(ns)` using `ns // 1000`.
53. **Broad `except Exception:` swallowing in header parse** *(pythonic)* — `__init__.py:797, 839`, `stream.py:1264`. Silently sets `headers=None`. At minimum log.
54. **`PullConsumer.get_info` is `async def` but never awaits** *(pythonic, API)* — `consumer/pull.py:521-523`. Docstring claims "refresh from server"; body returns cached `self._info`. Either fix the body or remove `async`.
55. **`OrderedConsumer.create` async factory** *(pythonic)* — instances created via `__init__` directly are half-initialized; every property guards on it. Make `__init__` private or initialize lazily.

## Low / polish

56. **`from __future__ import annotations` in all 9 files** *(pythonic)* — dead weight at 3.11+.
57. **`Union[...]` / `Optional[...]` / `Tuple[...]` from typing in `api/types.py`** *(pythonic)* — use `|`.
58. **`AsyncIterator` imported from `typing` in 5 files** *(pythonic)* — should be `collections.abc.AsyncIterator`. Also, `async def` generators return `AsyncGenerator[T, None]`, not `AsyncIterator[T]`. Affects `__init__.py:361, 392, 670, 703`, `stream.py:1021, 1029`.
59. **`StreamMessage.__getitem__`** *(API)* — `stream.py:1078-1082`. `dict`-style attribute access alongside attribute access. Footgun, also lets you read private attrs. Drop.
60. **`ErrorCode` as bare class** *(pythonic)* — should be `IntEnum` for `.name` / `.value` / iteration.
61. **`CONSUMER_ACTION_*` string literals** *(pythonic)* — should be `StrEnum`. Same for `Literal[...]` aliases (`AckPolicy`, `DeliverPolicy`, etc.) where runtime identity is useful.
62. **`MessageBatch.error: Exception | None`** — see #40.
63. **`__all__` doesn't re-export `Headers`, `Metadata`, `SequencePair`, `Message`, `MessageBatch`, `MessageStream`, `PullConsumer`, `OrderedConsumer`, or `headers.NATS_*`** *(API)* — users dig into submodules.
64. **`PublishAck.value` opaque field name** *(API)* — rename `counter_value`.
65. **`Stream.purge(filter=...)` shadows the Python builtin** *(API)* — `stream.py:1123`. Rename `subject` or `subject_filter`.
66. **Local imports inside `publish()`** *(pythonic)* — `__init__.py:314 import asyncio`, `:316 from nats.client.errors import NoRespondersError`. Lift to module level. Also `stream.py:1257`, `stream.py:1574` (which is dead code — already imported at module top).
67. **`JetStream.__init__` positional args** *(API)* — `(client, prefix, domain, strict)`. Make all but `client` keyword-only.
68. **Inconsistent return types: `delete_stream → bool`, `delete_consumer → bool`, `delete_message → None`, `pause_consumer → None`** *(API)* — pick `→ None` (raise on failure) everywhere.
69. **TODO `alternates` field never added to `StreamInfo`** *(smells)* — `stream.py:997`. Real lost data.
70. **`StreamManager` Protocol declares `create_stream(**config)`; impl takes positional `StreamConfig`** *(API)* — `stream.py:1017-1066`. Protocol is wrong.
71. **`consumer/__init__.py:125 priority_timeout: Any | None`** *(pythonic)* — `Any` while every other duration is `timedelta`.
72. **`timezone.utc` → `datetime.UTC` (3.11+)** *(pythonic)* — 6+ sites.
73. **Bare `pass` in retry/error paths with no logging** *(pythonic)* — `consumer/pull.py:444-448, 471-473`, `consumer/ordered.py:411`.
74. **`OrderedConsumer.__aexit__(self, *exc_info)` signature drift** *(pythonic)* — `consumer/ordered.py:144`. Use typed `(exc_type, exc_val, exc_tb)` like `pull.py:557`.
75. **Magic numbers / strings** *(smells)* — heartbeat 2x multiplier (`pull.py:78, 142, 277, 328, 468`), default batch 100, byte-mode batch 1_000_000 (`pull.py:653, 733`), backoff 1.0/10.0/2.0 (`ordered.py`), `$JS.ACK` parsing (`message.py:71-117`), `Nats-Pending-Messages/Bytes` (`pull.py:346, 348`).
76. **`Stream.__init__` accepts `info: StreamInfo | None`; `_info` nullable everywhere** *(API)* — force-fetch on init or split into `Stream` (always has info) vs `StreamRef`.
77. **`api/client.py:73-89 _error_from_response` is private but called from `__init__.py:342`** *(smells)* — public-by-use, private-by-name. Same for `is_error_response`.
78. **`SubjectTransform` typed as `Any` on `StreamSource`/`StreamSourceInfo`** *(ADR-36)* — round-trips OK, loses type safety. `stream.py:181, 257`.
79. **Inconsistent prefix on `nats.jetstream.api` logger** *(smells)* — hard-coded string vs `__name__` elsewhere.
80. **`set()` empties / lambda-style boilerplate in `check_response`** *(API)* — returns 3-tuple `(bool, set, set)` callers re-check field by field.

## Themes / leverage points

- **Dataclass marshaling layer (#18)** is the single highest-leverage
  cleanup. The generated `api/types.py` TypedDicts already describe the wire
  shape; the user-facing `@dataclass` mirrors plus their hand-rolled
  `from_response` / `to_request` (~400 lines across `consumer/__init__.py`
  and `stream.py`) re-encode the same fields by hand. Migrate the user-facing
  layer to `msgspec.Struct` (or extend the generator to emit it), and the
  silent-drop bugs and edit-three-places cost go away.
- **Pull consumer rewrite (#15, #4, #19, #5, #44)** is the biggest correctness
  cluster: polling → event-driven, fix 404/408 header semantics, surface
  errors properly instead of swallowing into `StopAsyncIteration`.
- **`Stream` god-class (#14, #22, #42)** — extracting `_api` to a typed
  protocol kills 11 RuntimeError guards. Split into `StreamManagement`,
  `StreamMessages`, `StreamConsumers` collaborators.
- **`OrderedConsumer` (#2, #13, #23, #41)** is largely broken or fragile:
  `fetch()` recreates per-call, `reset()` raises, invariants not enforced,
  exceptions silently swallowed. Worth a focused rewrite.
- **Publish ergonomics (#8, #9, #25, #20)** is the biggest user-facing gap —
  `msg_id`, `expected_*`, `ttl`, `publish_async`, batch publish. None of it
  exists despite the headers/types being half-wired.
- **Hard-coded `$JS.API` literals (#1, #38)** are real bugs for any
  domain/custom-prefix user. Single grep, ~6 sites.
