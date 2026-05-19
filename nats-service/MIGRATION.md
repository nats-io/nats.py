# Migrating from `nats.micro` to `nats.service`

`nats.service` is the modern service framework for Python 3.13+, layered on `nats-core`. This guide only covers the parts that are different from the legacy `nats.micro`. The wire protocol (`$SRV.PING`/`INFO`/`STATS` subjects, `io.nats.micro.v1.*_response` payload type tags, `Nats-Service-Error[-Code]` headers, the default `"q"` queue group) is unchanged, so a `nats.service` instance is fully interoperable with `nats.micro` callers and discovery tooling.

All packages share the `nats` namespace and can be installed side by side.

## Imports collapse into one module

```python
# Before
from nats.aio.client import Client
from nats.micro import add_service
from nats.micro.service import Service, ServiceConfig, ServiceVerb, control_subject
from nats.micro.request import Request, Handler, ServiceError

# After
from nats.client import Client
from nats.service import (
    add_service,
    Service,
    ServiceVerb,
    control_subject,
    Request,
    Handler,
    ServiceError,
)
```

- `nats.micro.service` and `nats.micro.request` are gone — everything lives directly under `nats.service`.
- `Client` comes from `nats.client`, not `nats.aio.client`.

## No `Config` dataclasses

```python
# Before
from nats.micro import add_service
from nats.micro.service import ServiceConfig, EndpointConfig, GroupConfig

service = await add_service(nc, ServiceConfig(name="echo", version="0.1.0"))
await service.add_endpoint(EndpointConfig(name="echo", handler=echo))
group = service.add_group(GroupConfig(name="v1"))

# After
from nats.service import add_service

service = await add_service(client, name="echo", version="0.1.0")
await service.add_endpoint(name="echo", handler=echo)
group = service.add_group("v1")
```

- `ServiceConfig`, `EndpointConfig`, `GroupConfig` and their `__post_init__` validators are gone. `add_service`, `add_endpoint`, and `add_group` accept the same fields as keyword arguments directly.
- `add_group` takes `name` positionally (it's the one required field); `queue_group` stays keyword-only.

## `ServiceError.code` is `int`, not `str`

```python
# Before
raise ServiceError("400", "bad request")
await request.respond_error("500", "boom")

# After
raise ServiceError(400, "bad request")
await request.respond_error(500, "boom")
```

The wire format is unchanged — the code is still serialized as a string in the `Nats-Service-Error-Code` header. Only the Python type changed.

## `respond` / `respond_error` accept multi-value headers

```python
# Before — headers: dict[str, str]
await request.respond(b"ok", headers={"X-Trace": "abc"})

# After — headers: dict[str, str | list[str]]
await request.respond(b"ok", headers={"X-Trace": "abc", "X-Tag": ["a", "b"]})
```

Matches the `Headers` type from `nats.client.message`. Single-string values still work.

## `reset()` is sync and actually clears stats

```python
# Before — only reset the started time, leaving num_requests etc. populated
await service.reset()

# After — resets per-endpoint counters as well as the started time
service.reset()
```

The legacy `reset()` was an `async` no-op that just overwrote `_started`. The modern version is synchronous and zeros `num_requests`, `num_errors`, `last_error`, and `processing_time` on every endpoint before resetting `started`.

## Opting out of queue groups

```python
# Before — no way to subscribe without a queue group; "q" was hard-applied
service = await add_service(nc, name="svc", version="0.1.0", queue_group="custom")

# After — pass NO_QUEUE_GROUP (or "") to subscribe directly, no queue
from nats.service import NO_QUEUE_GROUP

service = await add_service(client, name="svc", version="0.1.0", queue_group=NO_QUEUE_GROUP)
```

`queue_group=None` still defaults to `"q"`. The sentinel makes the "no queue at all" case expressible.

## Direct `Service(...)` is no longer the entry point

```python
# Before — Service could be constructed directly, then started
service = Service(nc, ServiceConfig(name="svc", version="0.1.0"))
await service.start()

# After — use the factory; Service is exposed only as the return type
service = await add_service(client, name="svc", version="0.1.0")
```

`Service.start()` is no longer public (the internal `_start` is invoked by `add_service`). The class itself is still importable for type hints / `isinstance` checks, but construction goes through `add_service`.

## Dataclasses no longer carry `to_dict` / `from_dict`

```python
# Before
payload = service.info().to_dict()
parsed = ServiceInfo.from_dict(json.loads(reply.data))

# After
from dataclasses import asdict

payload = asdict(service.info())
parsed = ServiceInfo(**json.loads(reply.data))  # or hand-roll if you need nested decoding
```

The legacy `to_dict`/`from_dict` helpers (which existed on `ServicePing`, `ServiceInfo`, `ServiceStats`, `EndpointInfo`, `EndpointStats`, `ServiceError`) are removed. Use `dataclasses.asdict` to encode, or decode by hand. The wire shape is unchanged.

## Server-side `started` is timezone-aware

The legacy implementation used `datetime.utcnow()` (naive, deprecated since 3.12). The modern one uses `datetime.now(timezone.utc)`. On the wire the `started` field is still an ISO-8601 string ending in `Z`, but the precision is now microseconds rather than milliseconds, and the in-memory `ServiceStats.started` value is now `tzinfo`-aware. Code that does `service.stats().started - other_aware_datetime` will start working; code that compared the naive value against another naive `datetime` needs to be updated to use timezone-aware values.

## `ServiceVerb` is a `Literal`, not an enum

```python
# Before
from nats.micro.service import ServiceVerb

subject = control_subject(ServiceVerb.PING, name="svc")

# After
subject = control_subject("PING", name="svc")
```

`ServiceVerb` is now a `Literal["PING", "INFO", "STATS"]` type alias rather than a `str, Enum` subclass. Pass the verb string directly. The alias stays importable so existing type annotations (`verb: ServiceVerb`) keep working.

## Service IDs are UUID4 hex, not NUID

Legacy IDs came from the client's NUID generator (22 chars). Modern IDs are `uuid.uuid4().hex` (32 chars). Anything that hardcoded the legacy length should be updated, but most consumers treat the ID as opaque.

## Error handling separates `ServiceError` from unhandled exceptions

The legacy endpoint dispatched both `ServiceError` and arbitrary `Exception` through the same `respond_error` path. The modern dispatcher:

- treats `ServiceError` as the structured-error path (`code`/`description` go into headers, `last_error` is `"<code>: <description>"`),
- routes other exceptions through a 500 response and logs them via `logger.exception(...)` on the `nats.service` logger,
- wraps the response publish in try/except so a failure to send the error reply is logged rather than swallowed.

If you previously raised plain `RuntimeError` and inspected the `Nats-Service-Error-Code` to detect 500 vs. user error, the behavior is the same; the logger output is new.
