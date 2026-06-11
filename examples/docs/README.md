# NATS Documentation Examples

These are the Python equivalents of the docs examples published in the NATS
documentation. They mirror the [Rust](../../../nats.rs/async-nats/examples)
and [Java](../../../nats.java/examples/src/main/java/io/nats/examples/natsIoDoc)
versions of the same examples.

The examples use the modern [`nats-core`](../../nats-core) client (and
[`nats-jetstream`](../../nats-jetstream) for the JetStream example).
They require **Python 3.13+**.

## Running

From the workspace root:

```bash
uv sync
uv run python examples/docs/basics_publish.py
```

Most examples connect to the public `demo.nats.io` server. The JetStream
example (`jetstream_basic.py`) needs a local server with JetStream enabled
on `127.0.0.1:4222` — for example:

```bash
nats-server -js
```

Examples that mix a publisher and subscriber include both in the same file
for convenience.

## Examples

### Basics
- `basics_publish.py` — publish a message to a subject
- `basics_subscribe.py` — subscribe to a subject

### Getting Started
- `getting_started_publish.py` — minimal publisher
- `getting_started_subscribe.py` — minimal subscriber (async + sync)

### Subjects
- `subjects_single_wildcard.py` — `*` token wildcard subscriptions
- `subjects_multi_wildcard.py` — `>` tail wildcard subscriptions
- `subjects_monitoring.py` — wire-tap subscription with `>`

### Queue Groups
- `queue_groups_basic.py` — three workers in a queue group
- `queue_groups_dynamic_scaling.py` — add/remove workers at runtime
- `queue_groups_mixed_subscribers.py` — mix of plain subs and queue subs
- `queue_groups_request_reply.py` — load-balanced request/reply

### Request / Reply
- `request_reply_basic.py` — basic request/reply
- `request_reply_calculator.py` — small calculator service
- `request_reply_headers.py` — request/reply with message headers
- `request_reply_multiple_responders.py` — multiple responders, first wins
- `request_reply_no_responders.py` — handling no-responders
- `request_reply_timeout.py` — request with a custom timeout

### JetStream
- `jetstream_basic.py` — create a stream, publish, durable pull consumer
