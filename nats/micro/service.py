# Copyright 2021-2022 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

import asyncio
import json
import secrets
import time

from datetime import datetime
from nats import NATS
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.micro import api
from nats.micro.request import Request, Verb, Handler, control_subject

def create_endpoint_stats(config: api.EndpointConfig) -> api.EndpointStats:
    return api.EndpointStats(
        name=config.name,
        subject=config.subject,
        num_requests=0,
        num_errors=0,
        last_error="",
        processing_time=0,
        average_processing_time=0,
        queue_group=config.queue_group,
        data={},
    )


def new_service_stats(
    id: str, started: datetime, config: api.ServiceConfig
) -> api.ServiceStats:
    return api.ServiceStats(
        name=config.name,
        id=id,
        version=config.version,
        started=started,
        endpoints=[],
        metadata=config.metadata,
    )


def create_endpoint_info(config: api.EndpointConfig) -> api.EndpointInfo:
    return api.EndpointInfo(
        name=config.name,
        subject=config.subject,
        metadata=config.metadata,
        queue_group=config.queue_group,
    )


def new_service_info(id: str, config: api.ServiceConfig) -> api.ServiceInfo:
    return api.ServiceInfo(
        name=config.name,
        id=id,
        version=config.version,
        description=config.description,
        metadata=config.metadata,
        endpoints=[],
    )


def new_ping_info(id: str, config: api.ServiceConfig) -> api.PingInfo:
    return api.PingInfo(
        name=config.name,
        id=id,
        version=config.version,
        metadata=config.metadata,
    )


def encode_ping_info(info: api.PingInfo) -> bytes:
    return json.dumps(info.as_dict(), separators=(",", ":")).encode()


def encode_stats(stats: api.ServiceStats) -> bytes:
    return json.dumps(stats.as_dict(), separators=(",", ":")).encode()


def encode_info(info: api.ServiceInfo) -> bytes:
    return json.dumps(info.as_dict(), separators=(",", ":")).encode()


def add_service(
    conn: NATS,
    name: str,
    version: str,
    description: str | None = None,
    metadata: dict[str, str] | None = None,
    queue_group: str | None = None,
    pending_bytes_limit_by_endpoint: int | None = None,
    pending_msgs_limit_by_endpoint: int | None = None,
    prefix: str = api.DEFAULT_PREFIX,
) -> Service:
    """Create a new service.

    A service is a collection of endpoints that are grouped together
    under a common name.

    Each endpoint is a request-reply handler for a subject.

    It's possible to add endpoints to a service after it has been created AND
    started.

    :param conn: The NATS client.
    :param name: The name of the service.
    :param version: The version of the service. Must be a valid semver version.
    :param description: The description of the service.
    :param metadata: The metadata of the service.
    :param queue_group: The default queue group of the service.
    :param pending_bytes_limit_by_endpoint: The default pending bytes limit for each endpoint within the service.
    :param pending_msgs_limit_by_endpoint: The default pending messages limit for each endpoint within the service.
    :param prefix: The prefix of the control subjects.
    """
    id = secrets.token_hex(16)
    config = api.ServiceConfig(
        name=name,
        version=version,
        description=description or "",
        metadata=metadata or {},
        queue_group=queue_group or api.DEFAULT_QUEUE_GROUP,
        pending_bytes_limit_by_endpoint=pending_bytes_limit_by_endpoint
        or api.DEFAULT_SUB_PENDING_BYTES_LIMIT,
        pending_msgs_limit_by_endpoint=pending_msgs_limit_by_endpoint
        or api.DEFAULT_SUB_PENDING_MSGS_LIMIT,
    )
    return Service(
        conn=conn,
        id=id,
        config=config,
        prefix=prefix,
    )


class Endpoint:
    """Endpoint manages a service endpoint."""

    def __init__(self, config: api.EndpointConfig) -> None:
        self.config = config
        self.stats = create_endpoint_stats(config)
        self.info = create_endpoint_info(config)
        self._sub: Subscription | None = None

    def reset(self) -> None:
        """Reset the endpoint statistics."""
        self.stats = create_endpoint_stats(self.config)
        self.info = create_endpoint_info(self.config)


class Group:
    """Group allows for grouping endpoints on a service.

    Endpoints created using `Group.add_endpoint` will be grouped
    under common prefix (group name). New groups can also be derived
    from a group using `Group.add_group`.
    """

    def __init__(self, config: GroupConfig, service: Service) -> None:
        self._config = config
        self._service = service

    def add_group(
        self,
        name: str,
        queue_group: str | None = None,
        pending_bytes_limit_by_endpoint: int | None = None,
        pending_msgs_limit_by_endpoint: int | None = None,
    ) -> Group:
        """Add a group to the group.

        :param name: The name of the group. Must be a valid NATS subject prefix.
        :param queue_group: The default queue group of the group. When queue group is not set, it defaults to the queue group of the parent group or service.
        :param pending_bytes_limit_by_endpoint: The default pending bytes limit for each endpoint within the group.
        :param pending_msgs_limit_by_endpoint: The default pending messages limit for each endpoint within the group.
        """
        config = self._config.child(
            name=name,
            queue_group=queue_group,
            pending_bytes_limit=pending_bytes_limit_by_endpoint,
            pending_msgs_limit=pending_msgs_limit_by_endpoint,
        )
        group = Group(config, self._service)
        return group

    async def add_endpoint(
        self,
        name: str,
        handler: Handler,
        subject: str | None = None,
        queue_group: str | None = None,
        metadata: dict[str, str] | None = None,
        pending_bytes_limit: int | None = None,
        pending_msgs_limit: int | None = None,
    ) -> Endpoint:
        """Add an endpoint to the group.

        :param name: The name of the endpoint.
        :param handler: The handler of the endpoint.
        :param subject: The subject of the endpoint. When subject is not set, it defaults to the name of the endpoint.
        :param queue_group: The queue group of the endpoint. When queue group is not set, it defaults to the queue group of the parent group or service.
        :param metadata: The metadata of the endpoint.
        :param pending_bytes_limit: The pending bytes limit for this endpoint.
        :param pending_msgs_limit: The pending messages limit for this endpoint.
        """
        return await self._service.add_endpoint(
            name=name,
            subject=f"{self._config.name}.{subject or name}",
            handler=handler,
            metadata=metadata,
            queue_group=queue_group or self._config.queue_group,
            pending_bytes_limit=pending_bytes_limit
            or self._config.pending_bytes_limit_by_endpoint,
            pending_msgs_limit=pending_msgs_limit
            or self._config.pending_msgs_limit_by_endpoint,
        )


def _get_service_subjects(
    verb: Verb,
    id: str,
    config: api.ServiceConfig,
    api_prefix: str,
) -> list[str]:
    """Get the internal subjects for a verb."""
    return [
        control_subject(verb, service=None, id=None, prefix=api_prefix),
        control_subject(
            verb, service=config.name, id=None, prefix=api_prefix
        ),
        control_subject(
            verb, service=config.name, id=id, prefix=api_prefix
        ),
    ]

class Service:
    """Services simplify the development of NATS micro-services.

    Endpoints can be added to a service after it has been created and started.
    Each endpoint is a request-reply handler for a subject.

    Groups can be added to a service to group endpoints under a common prefix.
    """

    def __init__(
        self,
        conn: NATS,
        id: str,
        config: api.ServiceConfig,
        prefix: str,
    ) -> None:
        self._nc = conn
        self._config = config
        self._api_prefix = prefix
        # Initialize state
        self._id = id
        self._endpoints: list[Endpoint] = []
        self._stopped = False
        # Internal responses
        self._stats = new_service_stats(self._id, datetime.now(), config)
        self._info = new_service_info(self._id, config)
        self._ping_response = new_ping_info(self._id, config)
        # Cache the serialized ping response
        self._ping_response_message = encode_ping_info(self._ping_response)
        # Internal subscriptions
        self._ping_subscriptions: list[Subscription] = []
        self._info_subscriptions: list[Subscription] = []
        self._stats_subscriptions: list[Subscription] = []

    async def start(self) -> None:
        """Start the service.

        A service MUST be started before adding endpoints.

        This will start the internal subscriptions and enable
        service discovery.
        """

        # Start PING subscriptions:
        # - $SRV.PING
        # - $SRV.{name}.PING
        # - $SRV.{name}.{id}.PING
        for subject in _get_service_subjects(
                Verb.PING,
                self._id,
                self._config,
                api_prefix=self._api_prefix,
        ):
            sub = await self._nc.subscribe(
                subject,
                cb=self._handle_ping_request,
            )
            self._ping_subscriptions.append(sub)
        # Start INFO subscriptions:
        # - $SRV.INFO
        # - $SRV.{name}.INFO
        # - $SRV.{name}.{id}.INFO
        for subject in _get_service_subjects(
                Verb.INFO,
                self._id,
                self._config,
                api_prefix=self._api_prefix,
        ):
            sub = await self._nc.subscribe(
                subject,
                cb=self._handle_info_request,
            )
            self._info_subscriptions.append(sub)
        # Start STATS subscriptions:
        # - $SRV.STATS
        # - $SRV.{name}.STATS
        # - $SRV.{name}.{id}.STATS
        for subject in _get_service_subjects(
                Verb.STATS,
                self._id,
                self._config,
                api_prefix=self._api_prefix,
        ):
            sub = await self._nc.subscribe(
                subject,
                cb=self._handle_stats_request,
            )
            self._stats_subscriptions.append(sub)

    async def stop(self) -> None:
        """Stop the service.

        This will stop all endpoints and internal subscriptions.
        """
        self._stopped = True
        # Stop all endpoints
        await asyncio.gather(*(_stop_endpoint(ep) for ep in self._endpoints))
        # Stop all internal subscriptions
        await asyncio.gather(
            *(
                _unsubscribe(sub) for subscriptions in (
                    self._stats_subscriptions,
                    self._info_subscriptions,
                    self._ping_subscriptions,
                ) for sub in subscriptions
            )
        )

    def stopped(self) -> bool:
        """Stopped informs whether [Stop] was executed on the service."""
        return self._stopped

    def info(self) -> api.ServiceInfo:
        """Returns the service info."""
        return self._info.copy()

    def stats(self) -> api.ServiceStats:
        """Returns statistics for the service endpoint and all monitoring endpoints."""
        return self._stats.copy()

    def reset(self) -> None:
        """Resets all statistics (for all endpoints) on a service instance."""

        # Internal responses
        self._stats = new_service_stats(self._id, datetime.now(), self._config)
        self._info = new_service_info(self._id, self._config)
        self._ping_response = new_ping_info(self._id, self._config)
        self._ping_response_message = encode_ping_info(self._ping_response)
        # Reset all endpoints
        endpoints = list(self._endpoints)
        self._endpoints.clear()
        for ep in endpoints:
            ep.reset()
            self._endpoints.append(ep)
            self._stats.endpoints.append(ep.stats)
            self._info.endpoints.append(ep.info)

    def add_group(
        self,
        name: str,
        queue_group: str | None = None,
        pending_bytes_limit_by_endpoint: int | None = None,
        pending_msgs_limit_by_endpoint: int | None = None,
    ) -> Group:
        """Add a group to the service.

        A group is a collection of endpoints that share the same prefix,
        and the same default queue group and pending limits.

        At runtime, a group does not exist as a separate entity, only
        endpoints exist. However, groups are useful to organize endpoints
        and to set default values for queue group and pending limits.

        :param name: The name of the group.
            queue_group: The default queue group of the group. When queue group is not set, it defaults to the queue group of the parent group or service.
            pending_bytes_limit_by_endpoint: The default pending bytes limit for each endpoint within the group.
            pending_msgs_limit_by_endpoint: The default pending messages limit for each endpoint within the group.
        """
        config = api.GroupConfig(
            name=name,
            queue_group=queue_group or self._config.queue_group,
            pending_bytes_limit_by_endpoint=pending_bytes_limit_by_endpoint
            or self._config.pending_bytes_limit_by_endpoint,
            pending_msgs_limit_by_endpoint=pending_msgs_limit_by_endpoint
            or self._config.pending_msgs_limit_by_endpoint,
        )
        return Group(config, self)

    async def add_endpoint(
        self,
        name: str,
        handler: Handler,
        subject: str | None = None,
        queue_group: str | None = None,
        metadata: dict[str, str] | None = None,
        pending_bytes_limit: int | None = None,
        pending_msgs_limit: int | None = None,
    ) -> Endpoint:
        """Add an endpoint to the service.

        An endpoint is a request-reply handler for a subject.

        :param name: The name of the endpoint.
        :param handler: The handler of the endpoint.
        :param subject: The subject of the endpoint. When subject is not set, it defaults to the name of the endpoint.
        :param queue_group: The queue group of the endpoint. When queue group is not set, it defaults to the queue group of the parent group or service.
        :param metadata: The metadata of the endpoint.
        :param pending_bytes_limit: The pending bytes limit for this endpoint.
        :param pending_msgs_limit: The pending messages limit for this endpoint.
        """
        if self._stopped:
            raise RuntimeError("Cannot add endpoint to a stopped service")
        config = self._config.endpoint_config(
            name=name,
            handler=handler,
            subject=subject,
            queue_group=queue_group,
            metadata=metadata,
            pending_bytes_limit=pending_bytes_limit,
            pending_msgs_limit=pending_msgs_limit,
        )
        # Create the endpoint
        ep = Endpoint(config)
        # Create the endpoint handler
        subscription_handler = _create_handler(ep)
        # Start the endpoint subscription
        subscription = await self._nc.subscribe(
            config.subject,
            queue=config.queue_group,
            cb=subscription_handler,
        )
        # Attach the subscription to the endpoint
        ep._sub = subscription
        # Append the endpoint to the service
        self._endpoints.append(ep)
        # Append the endpoint to the service stats and info
        self._stats.endpoints.append(ep.stats)
        self._info.endpoints.append(ep.info)
        return ep

    async def _handle_ping_request(self, msg: Msg) -> None:
        """Handle the ping message."""
        await msg.respond(data=self._ping_response_message)

    async def _handle_info_request(self, msg: Msg) -> None:
        """Handle the info message."""
        await msg.respond(data=encode_info(self._info))

    async def _handle_stats_request(self, msg: Msg) -> None:
        """Handle the stats message."""
        await msg.respond(data=encode_stats(self._stats))

    async def __aenter__(self) -> Service:
        """Implement the asynchronous context manager interface."""
        await self.start()
        return self

    async def __aexit__(self, *args: object, **kwargs: object) -> None:
        """Implement the asynchronous context manager interface."""
        await self.stop()


def _create_handler(
    endpoint: Endpoint
) -> Callable[[Msg], Awaitable[None]]:
    """A helper function called internally to create endpoint message handlers."""

    micro_handler = endpoint.config.handler

    async def handler(msg: Msg) -> None:
        start_time = time.perf_counter_ns()
        endpoint.stats.num_requests += 1
        request = Request(msg)
        try:
            await micro_handler(request)
        except Exception as exc:
            endpoint.stats.num_errors += 1
            endpoint.stats.last_error = repr(exc)
            await request.respond_error(
                code=500,
                description="Internal Server Error",
            )

        current_time = time.perf_counter_ns()
        elapsed_time = current_time - start_time

        endpoint.stats.processing_time += elapsed_time
        endpoint.stats.average_processing_time = int(
            endpoint.stats.processing_time / endpoint.stats.num_requests
        )

    return handler


async def _stop_endpoint(endpoint: Endpoint) -> None:
    """Stop the endpoint by draining its subscription."""
    if endpoint._sub:  # pyright: ignore[reportPrivateUsage]
        await _unsubscribe(endpoint._sub)  # pyright: ignore[reportPrivateUsage]
        endpoint._sub = None  # pyright: ignore[reportPrivateUsage]


async def _unsubscribe(sub: Subscription) -> None:
    try:
        await sub.unsubscribe()
    except BadSubscriptionError:
        pass
