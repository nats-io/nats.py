from asyncio import Event
from dataclasses import dataclass, replace, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.micro.api import DEFAULT_PREFIX, DEFAULT_QUEUE_GROUP
from typing import (
    Any,
    Dict,
    Optional,
    Protocol,
    List,
    overload,
    TypeAlias,
    Callable,
)

from nats.internal.data import Model

import json
import time


from .request import Request, Handler


class ServiceVerb(str, Enum):
    PING = "PING"
    STATS = "STATS"
    INFO = "INFO"


@dataclass
class EndpointConfig:
    name: str
    """An alphanumeric human-readable string used to describe the endpoint.

    Multiple endpoints can have the same names.
    """

    handler: Handler
    """The handler of the endpoint."""

    queue_group: Optional[str] = None
    """The queue group of the endpoint. When queue group is not set, it defaults to the queue group of the parent group or service."""

    subject: Optional[str] = None
    """The subject of the endpoint. When subject is not set, it defaults to the name of the endpoint."""

    metadata: Optional[Dict[str, str]] = None
    """The metadata of the endpoint."""


@dataclass
class EndpointStats(Model):
    """
    Statistics about a specific service endpoint
    """

    name: str
    """
    The endpoint name
    """

    subject: str
    """
    The subject the endpoint listens on
    """

    queue_group: str
    """
    The queue group this endpoint listens on for requests
    """

    num_requests: int = 0
    """
    The number of requests this endpoint has received
    """

    num_errors: int = 0
    """
    The number of errors this endpoint has encountered
    """

    last_error: Optional[str] = None
    """
    The last error the service encountered, if any.
    """

    processing_time: timedelta = timedelta()
    """
    How long, in total, was spent processing requests in the handler
    """
    average_processing_time: timedelta = timedelta()

    """
    Additional statistics the endpoint makes available
    """
    data: Optional[Dict[str, object]] = None


@dataclass
class EndpointInfo(Model):
    """The information of an endpoint."""

    name: str
    """
    The endopoint name
    """

    subject: str
    """
    The subject the endpoint listens on
    """

    queue_group: str
    """
    The queue group this endpoint listens on for requests
    """

    metadata: Optional[Dict[str, Any]] = None
    """
    The endpoint metadata.
    """


class Endpoint:
    """Endpoint manages a service endpoint."""

    def __init__(self, config: EndpointConfig) -> None:
        self._name = config.name
        self._subject = config.subject or config.name
        self._queue_group = config.queue_group or DEFAULT_QUEUE_GROUP
        self._handler = config.handler
        self._metadata = config.metadata

        self._num_requests = 0
        self._num_errors = 0
        self._processing_time = timedelta()
        self._average_processing_time = timedelta()
        self._last_error = None

        self._subscription: Optional[Subscription] = None

    async def _start(self, client: Client) -> None:
        assert not self._subscription
        self._subscription = await client.subscribe(
            subject=self._subject,
            queue=self._queue_group,
            cb=self._handle_request,
        )

    async def _stop(self) -> None:
        assert self._subscription
        await self._subscription.unsubscribe()
        self._subscription = None

    def _reset(self) -> None:
        """Reset the endpoint statistics."""
        self._num_requests = 0
        self._num_errors = 0

        self._processing_time = timedelta()
        self._average_processing_time = timedelta()

    async def _handle_request(self, msg: Msg) -> None:
        """Handle an endpoint message."""
        start_time = time.perf_counter()
        self._num_requests += 1
        request = Request(msg)
        try:
            await self._handler(request)
        except Exception as err:
            self._num_errors += 1
            self._last_error = repr(err)
            await request.respond_error(
                code=500,
                description="Internal Server Error",
            )
        finally:
            current_time = time.perf_counter()
            elapsed_time = current_time - start_time

            self._processing_time += timedelta(microseconds=elapsed_time)
            self._average_processing_time = (
                self._processing_time / self._num_requests
            )


@dataclass
class GroupConfig:
    """The configuration of a group."""

    prefix: str
    """The prefix of the group."""

    queue_group: Optional[str] = None
    """The default queue group of the group."""


class Group:
    def __init__(self, service: "Service", config: GroupConfig) -> None:
        self._service = service
        self._prefix = config.prefix
        self._queue_group = config.queue_group

    @overload
    async def add_endpoint(self, config: EndpointConfig) -> None: ...

    @overload
    async def add_endpoint(
        self,
        *,
        name: str,
        handler: Handler,
        queue_group: Optional[str] = None,
        subject: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None: ...

    async def add_endpoint(
        self, config: Optional[EndpointConfig] = None, **kwargs
    ) -> None:
        if config is None:
            config = EndpointConfig(**kwargs)
        else:
            config = replace(config, **kwargs)

        config = replace(config,
            subject = f"{self._prefix.strip('.')}.{config.subject or config.name}".strip('.'),
        )

        await self._service.add_endpoint(config)

    @overload
    def add_group(
        self, *, prefix: str, queue_group: Optional[str] = None
    ) -> "Group": ...

    @overload
    def add_group(self, config: GroupConfig) -> "Group": ...

    def add_group(self, config: Optional[GroupConfig] = None, **kwargs) -> "Group":
        if config is None:
            config = GroupConfig(**kwargs)
        else:
            config = replace(config, **kwargs)

        config = replace(config,
            prefix=f"{self._prefix}.{config.prefix}",
            queue_group=config.queue_group or self._queue_group,
        )

        return Group(self._service, config)


StatsHandler: TypeAlias = Callable[[EndpointStats], Any]
"""
A handler function used to configure a custom *STATS* endpoint.
"""


@dataclass
class ServiceConfig:
    """The configuration of a service."""

    name: str
    """The kind of the service. Shared by all services that have the same name.
    This name can only have A-Z, a-z, 0-9, dash, underscore."""

    version: str
    """The version of the service.
    This verson must be a valid semantic version."""

    description: Optional[str] = None
    """The description of the service."""

    metadata: Optional[Dict[str, Any]] = None
    """The metadata of the service."""

    queue_group: Optional[str] = None
    """The default queue group of the service."""

    stats_handler: Optional[StatsHandler] = None


class ServiceIdentity(Protocol):
    """
    Contains fields helping to identity a service instance.
    """

    name: str
    id: str
    version: str
    metadata: Optional[Dict[str, str]]


@dataclass
class ServicePing(Model, ServiceIdentity):
    """The response to a ping message."""

    name: str
    id: str
    version: str
    metadata: Optional[Dict[str, str]] = None
    type: str = "io.nats.micro.v1.ping_response"


@dataclass
class ServiceStats(Model, ServiceIdentity):
    """The statistics of a service."""

    name: str
    """
    The kind of the service. Shared by all the services that have the same name
    """
    id: str
    """
    A unique ID for this instance of a service
    """
    version: str
    """
    The version of the service
    """
    started: datetime
    """
    The time the service was started
    """
    endpoints: List[EndpointStats] = field(default_factory=list)
    """
    Statistics for each known endpoint
    """
    metadata: dict[str, str] | None = None
    """Service metadata."""

    type: str = "io.nats.micro.v1.stats_response"


@dataclass
class ServiceInfo(Model):
    """The information of a service."""

    name: str
    """
    The name of the service. Shared by all the services that have the same name
    """

    id: str
    """
    A unique ID for this instance of a service
    """

    version: str

    """
    The service metadata
    """
    endpoints: List[EndpointInfo] = field(default_factory=list)

    """
    The version of the service
    """
    description: Optional[str] = None
    """
    The description of the service supplied as configuration while creating the service
    """
    metadata: Optional[Dict[str, str]] = None

    """
    Information for all service endpoints
    """
    type: str = "io.nats.micro.v1.info_response"


class Service:
    def __init__(self, client: Client, config: ServiceConfig) -> None:
        self._id = client._nuid.next().decode()
        self._name = config.name
        self._version = config.version
        self._description = config.description
        self._metadata = config.metadata
        self._queue_group = config.queue_group
        self._stats_handler = config.stats_handler

        self._client = client
        self._subscriptions = {}
        self._endpoints = []
        self._started = datetime.now(UTC)
        self._stopped = Event()
        self._prefix = DEFAULT_PREFIX

    @property
    def id(self) -> str:
        """
        Returns the unique identifier of the service.
        """
        return self._id

    async def start(self) -> None:
        verb_request_handlers = {
            ServiceVerb.PING: self._handle_ping_request,
            ServiceVerb.INFO: self._handle_info_request,
            ServiceVerb.STATS: self._handle_stats_request,
        }

        for verb, verb_handler in verb_request_handlers.items():
            verb_subjects = [
                control_subject(verb, name=None, id=None, prefix=self._prefix),
                control_subject(
                    verb, name=self._name, id=None, prefix=self._prefix
                ),
                control_subject(
                    verb, name=self._name, id=self._id, prefix=self._prefix
                ),
            ]

            for subject in verb_subjects:
                self._subscriptions[subject] = await self._client.subscribe(
                    subject, cb=verb_handler
                )

        self._started = datetime.now()

    @overload
    async def add_endpoint(self, config: EndpointConfig) -> None: ...

    @overload
    async def add_endpoint(
        self,
        *,
        name: str,
        handler: Handler,
        queue_group: Optional[str] = None,
        subject: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None: ...

    async def add_endpoint(
        self, config: Optional[EndpointConfig] = None, **kwargs
    ) -> None:
        if config is None:
            config = EndpointConfig(**kwargs)
        else:
            config = replace(config, **kwargs)

        config = replace(config, queue_group=config.queue_group or self._queue_group)

        endpoint = Endpoint(config)
        await endpoint._start(self._client)
        self._endpoints.append(endpoint)

    @overload
    def add_group(self, *, prefix: str, queue_group: Optional[str] = None) -> Group: ...

    @overload
    def add_group(self, config: GroupConfig) -> Group: ...

    def add_group(self, config: Optional[GroupConfig] = None, **kwargs) -> Group:
        if config is None:
            config = GroupConfig(**kwargs)
        else:
            config = replace(config, **kwargs)

        config = replace(config, queue_group=config.queue_group or self._queue_group)

        return Group(self, config)

    def stats(self) -> ServiceStats:
        """
        Returns the statistics for the service.
        """
        stats = ServiceStats(
            id=self._id,
            name=self._name,
            version=self._version,
            metadata=self._metadata,
            endpoints=[
                EndpointStats(
                    name=endpoint._name,
                    subject=endpoint._subject,
                    queue_group=endpoint._queue_group,
                    num_requests=endpoint._num_requests,
                    num_errors=endpoint._num_errors,
                    last_error=endpoint._last_error,
                    processing_time=endpoint._processing_time,
                    average_processing_time=endpoint._average_processing_time,
                )
                for endpoint in (self._endpoints or [])
            ],
            started=self._started,
        )

        if self._stats_handler:
            for endpoint_stats in stats.endpoints:
                endpoint_stats.data = self._stats_handler(endpoint_stats)

        return stats

    def info(self) -> ServiceInfo:
        return ServiceInfo(
            id=self._id,
            name=self._name,
            version=self._version,
            metadata=self._metadata,
            description=self._description,
            endpoints=[
                EndpointInfo(
                    name=endpoint._name,
                    subject=endpoint._subject,
                    queue_group=endpoint._queue_group,
                    metadata=endpoint._metadata,
                )
                for endpoint in self._endpoints
            ],
        )

    async def reset(self) -> None:
        """
        Resets all statistics for all endpoints on a service instance.
        """
        self._started = datetime.utcnow()

    async def stop(self) -> None:
        if self._stopped.is_set():
            return

        for endpoint in self._endpoints:
            await endpoint._stop()

        for subscription in self._subscriptions.values():
            await subscription.drain()

        self._endpoints = []
        self._subscriptions = {}

        self._stopped.set()

    @property
    def stopped(self) -> Event:
        """
        Indicates whether `stop` was executed on the service.
        """
        return self._stopped

    async def __aenter__(self) -> "Service":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def _handle_ping_request(self, msg: Msg) -> None:
        """Handle a ping message."""
        ping = ServicePing(
            id=self._id,
            name=self._name,
            version=self._version,
            metadata=self._metadata,
        ).to_dict()

        await msg.respond(data=json.dumps(ping).encode())

    async def _handle_info_request(self, msg: Msg) -> None:
        """Handle an info message."""
        info = self.info().to_dict()

        await msg.respond(data=json.dumps(info).encode())

    async def _handle_stats_request(self, msg: Msg) -> None:
        """Handle a stats message."""
        stats = self.stats().to_dict()
        await msg.respond(data=json.dumps(stats).encode())


def control_subject(
    verb: ServiceVerb,
    name: Optional[str] = None,
    id: Optional[str] = None,
    prefix=DEFAULT_PREFIX,
) -> str:
    if name is None and id is None:
        return f"{prefix}.{verb.value}"
    elif id is None:
        return f"{prefix}.{verb.value}.{name}"
    else:
        return f"{prefix}.{verb.value}.{name}.{id}"
