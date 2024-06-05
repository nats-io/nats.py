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

from dataclasses import dataclass, fields, replace
from enum import Enum
from typing import Any, Awaitable, Callable, TypeVar

import datetime
import time
import secrets
import json

from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.aio.subscription import (
    DEFAULT_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_SUB_PENDING_MSGS_LIMIT,
    Subscription,
)
from nats.errors import BadSubscriptionError
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nats.micro.request import Handler

DEFAULT_QUEUE_GROUP = "q"
"""Queue Group name used across all services."""

DEFAULT_PREFIX = "$SRV"
"""APIPrefix is the root of all control subjects."""

_B = TypeVar("_B", bound="Base")


@dataclass
class Base:

    @classmethod
    def from_response(cls: type[_B], resp: dict[str, Any]) -> _B:
        """Read the class instance from a server response.

        Unknown fields are ignored ("open-world assumption").
        """
        params = {}
        for field in fields(cls):
            if field.name in resp:
                params[field.name] = resp[field.name]
        return cls(**params)

    def as_dict(self) -> dict[str, Any]:
        """Return the object converted into an API-friendly dict."""
        result: dict[str, Any] = {}
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            result[field.name] = val
        return result

    @staticmethod
    def _convert_rfc3339(resp: dict[str, Any], field: str) -> None:
        """Convert a RFC 3339 formatted string into a datetime.
        If the string is None, None is returned.
        """
        val = resp.get(field, None)
        if val is None:
            return None
        raw_date = val[:26]
        if raw_date.endswith("Z"):
            raw_date = raw_date[:-1] + "+00:00"
        resp[field] = datetime.datetime.fromisoformat(raw_date).replace(
            tzinfo=datetime.timezone.utc
        )

    @staticmethod
    def _to_rfc3339(date: datetime.datetime) -> str:
        """Convert a datetime into RFC 3339 formatted string.
        If datetime does not have timezone information, datetime
        is assumed to be in UTC timezone.
        """
        if date.tzinfo is None:
            date = date.replace(tzinfo=datetime.timezone.utc)
        elif date.tzinfo != datetime.timezone.utc:
            date = date.astimezone(datetime.timezone.utc)
        return date.isoformat().replace("+00:00", "Z").replace(".000000", "")


@dataclass
class EndpointStats(Base):
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
    num_requests: int
    """
    The number of requests this endpoint received
    """
    num_errors: int
    """
    The number of errors this endpoint encountered
    """
    last_error: str
    """
    The last error the service encountered
    """
    processing_time: int
    """
    How long, in total, was spent processing requests in the handler
    """
    average_processing_time: int
    """
    The average time spent processing requests
    """
    queue_group: str | None = None
    """
    The queue group this endpoint listens on for requests
    """
    data: dict[str, object] | None = None
    """
    Additional statistics the endpoint makes available
    """

    def copy(self) -> EndpointStats:
        return replace(
            self, data=None if self.data is None else self.data.copy()
        )


@dataclass
class ServiceStats(Base):
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
    started: datetime.datetime
    """
    The time the service was stated in RFC3339 format
    """
    endpoints: list[EndpointStats]
    """
    Statistics for each known endpoint
    """
    metadata: dict[str, str] | None = None
    """Service metadata."""

    type: str = "io.nats.micro.v1.stats_response"

    def copy(self) -> ServiceStats:
        return replace(
            self,
            endpoints=[ep.copy() for ep in self.endpoints],
            metadata=None if self.metadata is None else self.metadata.copy(),
        )

    def as_dict(self) -> dict[str, Any]:
        """Return the object converted into an API-friendly dict."""
        result = super().as_dict()
        result["endpoints"] = [ep.as_dict() for ep in self.endpoints]
        result["started"] = self._to_rfc3339(self.started)
        return result

    @classmethod
    def from_response(cls, resp: dict[str, Any]) -> ServiceStats:
        """Read the class instance from a server response.

        Unknown fields are ignored ("open-world assumption").
        """
        cls._convert_rfc3339(resp, "started")
        stats = super().from_response(resp)
        stats.endpoints = [
            EndpointStats.from_response(ep) for ep in resp["endpoints"]
        ]
        return stats


@dataclass
class EndpointInfo(Base):
    """The information of an endpoint."""

    name: str
    """
    The endopoint name
    """
    subject: str
    """
    The subject the endpoint listens on
    """
    metadata: dict[str, str] | None = None
    """
    The endpoint metadata.
    """
    queue_group: str | None = None
    """
    The queue group this endpoint listens on for requests
    """

    def copy(self) -> EndpointInfo:
        return replace(
            self,
            metadata=None if self.metadata is None else self.metadata.copy(),
        )


@dataclass
class ServiceInfo(Base):
    """The information of a service."""

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
    description: str
    """
    The description of the service supplied as configuration while creating the service
    """
    metadata: dict[str, str]
    """
    The service metadata
    """
    endpoints: list[EndpointInfo]
    """
    Information for all service endpoints
    """
    type: str = "io.nats.micro.v1.info_response"

    def copy(self) -> ServiceInfo:
        return replace(
            self,
            endpoints=[ep.copy() for ep in self.endpoints],
            metadata=self.metadata.copy(),
        )

    def as_dict(self) -> dict[str, Any]:
        """Return the object converted into an API-friendly dict."""
        result = super().as_dict()
        result["endpoints"] = [ep.as_dict() for ep in self.endpoints]
        return result

    @classmethod
    def from_response(cls, resp: dict[str, Any]) -> ServiceInfo:
        """Read the class instance from a server response.

        Unknown fields are ignored ("open-world assumption").
        """
        info = super().from_response(resp)
        info.endpoints = [EndpointInfo(**ep) for ep in resp["endpoints"]]
        return info


@dataclass
class PingInfo(Base):
    """The response to a ping message."""

    name: str
    id: str
    version: str
    metadata: dict[str, str]
    type: str = "io.nats.micro.v1.ping_response"

    def copy(self) -> PingInfo:
        return replace(self, metadata=self.metadata.copy())


@dataclass
class ServiceConfig:
    """The configuration of a service."""

    name: str
    """The kind of the service. Shared by all services that have the same name.
    This name can only have A-Z, a-z, 0-9, dash, underscore."""

    version: str
    """The version of the service.
    This verson must be a valid semantic version."""

    description: str
    """The description of the service."""

    metadata: dict[str, str]
    """The metadata of the service."""

    queue_group: str
    """The default queue group of the service."""

    pending_msgs_limit_by_endpoint: int
    """The default pending messages limit of the service.

    This limit is applied BY subject.
    """

    pending_bytes_limit_by_endpoint: int
    """The default pending bytes limit of the service.

    This limit is applied BY subject.
    """

    def endpoint_config(
        self,
        name: str,
        handler: Handler,
        subject: str | None = None,
        queue_group: str | None = None,
        metadata: dict[str, str] | None = None,
        pending_bytes_limit: int | None = None,
        pending_msgs_limit: int | None = None,
    ) -> EndpointConfig:
        return EndpointConfig(
            name=name,
            subject=subject or name,
            handler=handler,
            metadata=metadata or {},
            queue_group=queue_group or self.queue_group,
            pending_bytes_limit=pending_bytes_limit
            or self.pending_bytes_limit_by_endpoint,
            pending_msgs_limit=pending_msgs_limit
            or self.pending_msgs_limit_by_endpoint,
        )


@dataclass
class EndpointConfig:
    name: str
    """An alphanumeric human-readable string used to describe the endpoint.

    Multiple endpoints can have the same names.
    """

    subject: str
    """The subject of the endpoint. When subject is not set, it defaults to the name of the endpoint."""

    handler: Handler
    """The handler of the endpoint."""

    queue_group: str
    """The queue group of the endpoint. When queue group is not set, it defaults to the queue group of the parent group or service."""

    metadata: dict[str, str]
    """The metadata of the endpoint."""

    pending_msgs_limit: int
    """The pending message limit for this endpoint."""

    pending_bytes_limit: int
    """The pending bytes limit for this endpoint."""


@dataclass
class GroupConfig:
    """The configuration of a group."""

    name: str
    """The name of the group.
    Group names cannot contain '>' wildcard (as group name serves as subject prefix)."""

    queue_group: str
    """The default queue group of the group."""

    pending_msgs_limit_by_endpoint: int
    """The default pending message limit for each endpoint within the group."""

    pending_bytes_limit_by_endpoint: int
    """The default pending bytes limit for each endpoint within the group."""

    def child(
        self,
        name: str,
        queue_group: str | None = None,
        pending_bytes_limit: int | None = None,
        pending_msgs_limit: int | None = None,
    ) -> GroupConfig:
        return GroupConfig(
            name=f"{self.name}.{name}",
            queue_group=queue_group or self.queue_group,
            pending_bytes_limit_by_endpoint=pending_bytes_limit
            or self.pending_bytes_limit_by_endpoint,
            pending_msgs_limit_by_endpoint=pending_msgs_limit
            or self.pending_msgs_limit_by_endpoint,
        )
