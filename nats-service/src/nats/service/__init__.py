"""NATS service framework (ADR-32).

Provides a higher-level abstraction over NATS subscriptions for building
discoverable, observable services. A service exposes user-defined endpoints
under arbitrary subjects, while the framework registers control endpoints
under ``$SRV.PING``, ``$SRV.INFO`` and ``$SRV.STATS`` for ping, info and
stats responses.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal, Self

from nats.client.message import Headers, Message
from nats.service.errors import ServiceError

if TYPE_CHECKING:
    import types

    from nats.client import Client
    from nats.client.subscription import Subscription

logger = logging.getLogger("nats.service")


DEFAULT_PREFIX = "$SRV"
"""Root of the control subject hierarchy."""

DEFAULT_QUEUE_GROUP = "q"
"""Queue group applied to endpoints when neither the endpoint nor its parent overrides it."""

NO_QUEUE_GROUP = ""
"""Sentinel passed as ``queue_group`` to opt out of queue subscriptions."""

ERROR_HEADER = "Nats-Service-Error"
ERROR_CODE_HEADER = "Nats-Service-Error-Code"

PING_RESPONSE_TYPE = "io.nats.micro.v1.ping_response"
INFO_RESPONSE_TYPE = "io.nats.micro.v1.info_response"
STATS_RESPONSE_TYPE = "io.nats.micro.v1.stats_response"

# Official SemVer regex: https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)
_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_SUBJECT_RE = re.compile(r"^[^ >]*[>]?$")


ServiceVerb = Literal["PING", "INFO", "STATS"]
"""Control verbs that a service responds to under ``$SRV``."""


def control_subject(
    verb: ServiceVerb, *, name: str | None = None, id: str | None = None, prefix: str = DEFAULT_PREFIX
) -> str:
    """Build a ``$SRV`` control subject for the given verb."""
    if name is None and id is None:
        return f"{prefix}.{verb}"
    if id is None:
        return f"{prefix}.{verb}.{name}"
    return f"{prefix}.{verb}.{name}.{id}"


@dataclass(slots=True)
class EndpointInfo:
    """Discovery information about a single endpoint."""

    name: str
    subject: str
    queue_group: str
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class EndpointStats:
    """Runtime statistics about a single endpoint."""

    name: str
    subject: str
    queue_group: str
    num_requests: int = 0
    num_errors: int = 0
    last_error: str = ""
    processing_time: int = 0
    average_processing_time: int = 0
    data: object | None = None


@dataclass(slots=True)
class ServicePing:
    """``$SRV.PING`` response payload."""

    name: str
    id: str
    version: str
    metadata: dict[str, str] = field(default_factory=dict)
    type: str = PING_RESPONSE_TYPE


@dataclass(slots=True)
class ServiceInfo:
    """``$SRV.INFO`` response payload."""

    name: str
    id: str
    version: str
    description: str
    metadata: dict[str, str] = field(default_factory=dict)
    endpoints: list[EndpointInfo] = field(default_factory=list)
    type: str = INFO_RESPONSE_TYPE


@dataclass(slots=True)
class ServiceStats:
    """``$SRV.STATS`` response payload."""

    name: str
    id: str
    version: str
    started: datetime
    metadata: dict[str, str] = field(default_factory=dict)
    endpoints: list[EndpointStats] = field(default_factory=list)
    type: str = STATS_RESPONSE_TYPE


Handler = Callable[["Request"], Awaitable[None]]
"""Coroutine invoked for every request received by an endpoint."""

StatsHandler = Callable[[EndpointStats], object]
"""Optional hook returning user-defined data attached to each endpoint's stats.

The returned value must be JSON-serializable, as it is embedded verbatim in
the ``$SRV.STATS`` response payload.
"""


class Request:
    """A request received by a service endpoint."""

    __slots__ = ("_message", "_client")

    def __init__(self, message: Message, client: Client) -> None:
        self._message = message
        self._client = client

    @property
    def subject(self) -> str:
        return self._message.subject

    @property
    def data(self) -> bytes:
        return self._message.data

    @property
    def reply(self) -> str | None:
        return self._message.reply

    @property
    def headers(self) -> Headers | None:
        return self._message.headers

    async def respond(self, data: bytes = b"", *, headers: dict[str, str | list[str]] | None = None) -> None:
        """Publish a reply to the request."""
        if not self._message.reply:
            raise RuntimeError("request has no reply subject")
        await self._client.publish(self._message.reply, data, headers=headers)

    async def respond_error(
        self,
        code: int,
        description: str,
        data: bytes = b"",
        *,
        headers: dict[str, str | list[str]] | None = None,
    ) -> None:
        """Publish a structured error reply."""
        merged: dict[str, str | list[str]] = dict(headers) if headers else {}
        merged[ERROR_HEADER] = description
        merged[ERROR_CODE_HEADER] = str(code)
        await self.respond(data, headers=merged)


def _validate_name(name: str) -> None:
    if not name:
        raise ValueError("name cannot be empty")
    if not _NAME_RE.match(name):
        raise ValueError("name must contain only alphanumeric characters, dashes, and underscores")


def _validate_version(version: str) -> None:
    if not version:
        raise ValueError("version cannot be empty")
    if not _SEMVER_RE.match(version):
        raise ValueError("version must be a valid semantic version")


def _validate_subject(subject: str) -> None:
    if not subject:
        raise ValueError("subject cannot be empty")
    if not _SUBJECT_RE.match(subject):
        raise ValueError("subject must not contain spaces and can only have '>' at the end")


class _Endpoint:
    """Internal endpoint state. Not part of the public API."""

    __slots__ = (
        "name",
        "subject",
        "queue_group",
        "metadata",
        "handler",
        "num_requests",
        "num_errors",
        "last_error",
        "processing_time",
        "_service",
        "_subscription",
        "_task",
    )

    def __init__(
        self,
        service: Service,
        *,
        name: str,
        subject: str,
        queue_group: str,
        metadata: dict[str, str],
        handler: Handler,
    ) -> None:
        self._service = service
        self.name = name
        self.subject = subject
        self.queue_group = queue_group
        self.metadata = metadata
        self.handler = handler
        self.num_requests = 0
        self.num_errors = 0
        self.last_error = ""
        self.processing_time = 0
        self._subscription: Subscription | None = None
        self._task: asyncio.Task[None] | None = None

    async def _start(self) -> None:
        self._subscription = await self._service._client.subscribe(self.subject, queue=self.queue_group)
        self._task = asyncio.create_task(self._serve(), name=f"nats-service-endpoint:{self.subject}")

    async def _serve(self) -> None:
        assert self._subscription is not None
        async for message in self._subscription:
            await self._handle(message)

    async def _handle(self, message: Message) -> None:
        request = Request(message, self._service._client)
        self.num_requests += 1
        start = time.perf_counter_ns()
        try:
            await self.handler(request)
        except ServiceError as error:
            self.num_errors += 1
            self.last_error = f"{error.code}: {error.description}"
            if message.reply:
                try:
                    await request.respond_error(error.code, error.description)
                except Exception:
                    logger.exception("failed to send error response for %s", self.subject)
        except Exception as error:
            self.num_errors += 1
            self.last_error = repr(error)
            logger.exception("unhandled exception in endpoint %s", self.subject)
            if message.reply:
                try:
                    await request.respond_error(500, "internal error")
                except Exception:
                    logger.exception("failed to send error response for %s", self.subject)
        finally:
            self.processing_time += time.perf_counter_ns() - start

    async def _stop(self) -> None:
        if self._subscription is not None:
            await self._subscription.drain()
        if self._task is not None:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("endpoint task for %s exited with an error", self.subject)

    def _reset(self) -> None:
        self.num_requests = 0
        self.num_errors = 0
        self.last_error = ""
        self.processing_time = 0


class Group:
    """A subject prefix under which endpoints can be registered."""

    __slots__ = ("_service", "_prefix", "_queue_group")

    def __init__(self, service: Service, prefix: str, queue_group: str | None) -> None:
        self._service = service
        self._prefix = prefix
        self._queue_group = queue_group

    @property
    def name(self) -> str:
        return self._prefix

    async def add_endpoint(
        self,
        *,
        name: str,
        handler: Handler,
        subject: str | None = None,
        queue_group: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Register an endpoint whose subject is prefixed with this group's name."""
        endpoint_subject = subject if subject is not None else name
        full_subject = f"{self._prefix}.{endpoint_subject}" if self._prefix else endpoint_subject
        await self._service._register_endpoint(
            name=name,
            subject=full_subject,
            queue_group=queue_group if queue_group is not None else self._queue_group,
            metadata=metadata,
            handler=handler,
        )

    def add_group(self, name: str, *, queue_group: str | None = None) -> Group:
        """Create a nested group whose prefix extends this group's prefix."""
        if ">" in name:
            raise ValueError("group name cannot contain '>'")
        prefix = f"{self._prefix}.{name}" if self._prefix else name
        return Group(self._service, prefix, queue_group if queue_group is not None else self._queue_group)


class Service(AbstractAsyncContextManager["Service"]):
    """A NATS service instance.

    Returned by :func:`add_service`. Use :meth:`add_endpoint` and
    :meth:`add_group` to register handlers, and :meth:`stop` (or the async
    context manager) to drain subscriptions when the service exits.
    """

    __slots__ = (
        "_client",
        "_id",
        "_name",
        "_version",
        "_description",
        "_metadata",
        "_queue_group",
        "_stats_handler",
        "_prefix",
        "_started",
        "_stopped",
        "_endpoints",
        "_control_subscriptions",
        "_control_tasks",
    )

    def __init__(
        self,
        client: Client,
        *,
        name: str,
        version: str,
        description: str = "",
        metadata: dict[str, str] | None = None,
        queue_group: str | None = None,
        stats_handler: StatsHandler | None = None,
        prefix: str = DEFAULT_PREFIX,
    ) -> None:
        _validate_name(name)
        _validate_version(version)
        if queue_group is not None and queue_group != NO_QUEUE_GROUP:
            _validate_subject(queue_group)

        self._client = client
        self._id = uuid.uuid4().hex
        self._name = name
        self._version = version
        self._description = description
        self._metadata: dict[str, str] = dict(metadata) if metadata else {}
        self._queue_group = queue_group if queue_group is not None else DEFAULT_QUEUE_GROUP
        self._stats_handler = stats_handler
        self._prefix = prefix
        self._started = datetime.now(timezone.utc)
        self._stopped = asyncio.Event()
        self._endpoints: list[_Endpoint] = []
        self._control_subscriptions: list[Subscription] = []
        self._control_tasks: list[asyncio.Task[None]] = []

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def stopped(self) -> asyncio.Event:
        """Event set once :meth:`stop` has finished draining."""
        return self._stopped

    async def _start(self) -> None:
        handlers: dict[ServiceVerb, Callable[[Message], Awaitable[None]]] = {
            "PING": self._handle_ping,
            "INFO": self._handle_info,
            "STATS": self._handle_stats,
        }
        for verb, handler in handlers.items():
            for subject in (
                control_subject(verb, prefix=self._prefix),
                control_subject(verb, name=self._name, prefix=self._prefix),
                control_subject(verb, name=self._name, id=self._id, prefix=self._prefix),
            ):
                subscription = await self._client.subscribe(subject)
                self._control_subscriptions.append(subscription)
                task = asyncio.create_task(
                    self._control_loop(subscription, handler),
                    name=f"nats-service-control:{subject}",
                )
                self._control_tasks.append(task)

    async def _control_loop(self, subscription: Subscription, handler: Callable[[Message], Awaitable[None]]) -> None:
        async for message in subscription:
            try:
                await handler(message)
            except Exception:
                logger.exception("error while handling control message on %s", subscription.subject)

    async def add_endpoint(
        self,
        *,
        name: str,
        handler: Handler,
        subject: str | None = None,
        queue_group: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Register a new endpoint on the service."""
        await self._register_endpoint(
            name=name,
            subject=subject if subject is not None else name,
            queue_group=queue_group,
            metadata=metadata,
            handler=handler,
        )

    def add_group(self, name: str, *, queue_group: str | None = None) -> Group:
        """Create a group whose name will prefix the subject of any endpoint registered on it."""
        if ">" in name:
            raise ValueError("group name cannot contain '>'")
        return Group(self, name, queue_group if queue_group is not None else self._queue_group)

    async def _register_endpoint(
        self,
        *,
        name: str,
        subject: str,
        queue_group: str | None,
        metadata: dict[str, str] | None,
        handler: Handler,
    ) -> None:
        if self._stopped.is_set():
            raise RuntimeError("service is stopped")
        _validate_name(name)
        _validate_subject(subject)
        resolved_queue_group = queue_group if queue_group is not None else self._queue_group
        if resolved_queue_group != NO_QUEUE_GROUP:
            _validate_subject(resolved_queue_group)
        endpoint = _Endpoint(
            self,
            name=name,
            subject=subject,
            queue_group=resolved_queue_group,
            metadata=dict(metadata) if metadata else {},
            handler=handler,
        )
        await endpoint._start()
        self._endpoints.append(endpoint)

    def info(self) -> ServiceInfo:
        """Snapshot of the discovery info advertised by the service."""
        return ServiceInfo(
            name=self._name,
            id=self._id,
            version=self._version,
            description=self._description,
            metadata=dict(self._metadata),
            endpoints=[
                EndpointInfo(
                    name=endpoint.name,
                    subject=endpoint.subject,
                    queue_group=endpoint.queue_group,
                    metadata=dict(endpoint.metadata),
                )
                for endpoint in self._endpoints
            ],
        )

    def stats(self) -> ServiceStats:
        """Snapshot of the runtime statistics for the service."""
        endpoints: list[EndpointStats] = []
        for endpoint in self._endpoints:
            average = endpoint.processing_time // endpoint.num_requests if endpoint.num_requests else 0
            stat = EndpointStats(
                name=endpoint.name,
                subject=endpoint.subject,
                queue_group=endpoint.queue_group,
                num_requests=endpoint.num_requests,
                num_errors=endpoint.num_errors,
                last_error=endpoint.last_error,
                processing_time=endpoint.processing_time,
                average_processing_time=average,
            )
            if self._stats_handler is not None:
                stat.data = self._stats_handler(stat)
            endpoints.append(stat)
        return ServiceStats(
            name=self._name,
            id=self._id,
            version=self._version,
            started=self._started,
            metadata=dict(self._metadata),
            endpoints=endpoints,
        )

    def reset(self) -> None:
        """Reset the runtime statistics for every endpoint."""
        for endpoint in self._endpoints:
            endpoint._reset()
        self._started = datetime.now(timezone.utc)

    async def stop(self) -> None:
        """Drain all subscriptions and mark the service as stopped."""
        if self._stopped.is_set():
            return

        for endpoint in self._endpoints:
            await endpoint._stop()

        for subscription in self._control_subscriptions:
            await subscription.drain()
        for task in self._control_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("control task %s exited with an error", task.get_name())

        self._stopped.set()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self.stop()

    async def _handle_ping(self, message: Message) -> None:
        if not message.reply:
            return
        ping = ServicePing(
            name=self._name,
            id=self._id,
            version=self._version,
            metadata=dict(self._metadata),
        )
        await self._client.publish(message.reply, json.dumps(asdict(ping)).encode())

    async def _handle_info(self, message: Message) -> None:
        if not message.reply:
            return
        await self._client.publish(message.reply, json.dumps(asdict(self.info())).encode())

    async def _handle_stats(self, message: Message) -> None:
        if not message.reply:
            return
        stats = self.stats()
        payload = asdict(stats)
        payload["started"] = stats.started.isoformat().replace("+00:00", "Z")
        try:
            body = json.dumps(payload).encode()
        except (TypeError, ValueError):
            logger.exception("failed to serialize stats payload for %s", self._name)
            return
        await self._client.publish(message.reply, body)


async def add_service(
    client: Client,
    *,
    name: str,
    version: str,
    description: str = "",
    metadata: dict[str, str] | None = None,
    queue_group: str | None = None,
    stats_handler: StatsHandler | None = None,
    prefix: str = DEFAULT_PREFIX,
) -> Service:
    """Create and start a new :class:`Service` on the given client."""
    service = Service(
        client,
        name=name,
        version=version,
        description=description,
        metadata=metadata,
        queue_group=queue_group,
        stats_handler=stats_handler,
        prefix=prefix,
    )
    await service._start()
    return service


__all__ = [
    "DEFAULT_PREFIX",
    "DEFAULT_QUEUE_GROUP",
    "NO_QUEUE_GROUP",
    "ERROR_HEADER",
    "ERROR_CODE_HEADER",
    "PING_RESPONSE_TYPE",
    "INFO_RESPONSE_TYPE",
    "STATS_RESPONSE_TYPE",
    "ServiceVerb",
    "control_subject",
    "EndpointInfo",
    "EndpointStats",
    "ServicePing",
    "ServiceInfo",
    "ServiceStats",
    "Handler",
    "StatsHandler",
    "Request",
    "Group",
    "Service",
    "ServiceError",
    "add_service",
]
