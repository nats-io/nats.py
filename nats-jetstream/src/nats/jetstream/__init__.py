"""JetStream package."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, AsyncIterator, overload

from nats.client.message import Headers
from nats.client.protocol.message import parse_headers
from nats.jetstream import api
from nats.jetstream.consumer import Consumer, ConsumerInfo
from nats.jetstream.stream import (
    ClusterInfo,
    ExternalStreamSource,
    PeerInfo,
    Placement,
    Republish,
    Stream,
    StreamConfig,
    StreamConsumerLimits,
    StreamInfo,
    StreamMessage,
    StreamSource,
    StreamSourceInfo,
    StreamState,
    SubjectTransform,
)

if TYPE_CHECKING:
    from nats.client import Client


@dataclass
class APIStats:
    """JetStream API statistics."""

    total: int
    errors: int
    level: str | None = None
    """The API level."""

    @classmethod
    def from_response(cls, data: api.ApiStats, *, strict: bool = False) -> APIStats:
        total = data.pop("total")
        errors = data.pop("errors")
        level = data.pop("level", None)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"APIStats.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            total=total,
            errors=errors,
            level=level,
        )


@dataclass
class AccountLimits:
    """JetStream account limits."""

    max_memory: int
    max_storage: int
    max_streams: int
    max_consumers: int
    max_ack_pending: int
    memory_max_stream_bytes: int
    storage_max_stream_bytes: int
    max_bytes_required: bool

    @classmethod
    def from_response(cls, data: api.AccountLimits, *, strict: bool = False) -> AccountLimits:
        max_memory = data.pop("max_memory")
        max_storage = data.pop("max_storage")
        max_streams = data.pop("max_streams")
        max_consumers = data.pop("max_consumers")
        max_ack_pending = data.pop("max_ack_pending")
        memory_max_stream_bytes = data.pop("memory_max_stream_bytes")
        storage_max_stream_bytes = data.pop("storage_max_stream_bytes")
        max_bytes_required = data.pop("max_bytes_required")

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"AccountLimits.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            max_memory=max_memory,
            max_storage=max_storage,
            max_streams=max_streams,
            max_consumers=max_consumers,
            max_ack_pending=max_ack_pending,
            memory_max_stream_bytes=memory_max_stream_bytes,
            storage_max_stream_bytes=storage_max_stream_bytes,
            max_bytes_required=max_bytes_required,
        )


@dataclass
class Tier:
    """JetStream account tier."""

    memory: int
    storage: int
    streams: int
    consumers: int
    limits: AccountLimits

    @classmethod
    def from_response(cls, data: api.Tier, *, strict: bool = False) -> Tier:
        memory = data.pop("memory")
        storage = data.pop("storage")
        streams = data.pop("streams")
        consumers = data.pop("consumers")
        limits = AccountLimits.from_response(data.pop("limits"), strict=strict)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"Tier.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            memory=memory,
            storage=storage,
            streams=streams,
            consumers=consumers,
            limits=limits,
        )


@dataclass
class AccountInfo:
    """JetStream account information."""

    memory: int
    storage: int
    streams: int
    consumers: int
    limits: AccountLimits
    api: APIStats
    domain: str | None
    tiers: dict[str, Tier] | None
    reserved_memory: int | None = None
    """Reserved memory for the account."""
    reserved_storage: int | None = None
    """Reserved storage for the account."""

    @classmethod
    def from_response(cls, data: api.AccountInfo, *, strict: bool = False) -> AccountInfo:
        memory = data.pop("memory")
        storage = data.pop("storage")
        streams = data.pop("streams")
        consumers = data.pop("consumers")
        limits = AccountLimits.from_response(data.pop("limits"), strict=strict)
        api_stats = APIStats.from_response(data.pop("api"), strict=strict)
        domain = data.pop("domain", None)

        tiers = None
        tiers_data = data.pop("tiers", None)
        if tiers_data:
            tiers = {k: Tier.from_response(v, strict=strict) for k, v in tiers_data.items()}

        reserved_memory = data.pop("reserved_memory", None)
        reserved_storage = data.pop("reserved_storage", None)

        # Pop response envelope fields that aren't part of AccountInfo
        data.pop("type", None)  # Response type discriminator

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"AccountInfo.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            memory=memory,
            storage=storage,
            streams=streams,
            consumers=consumers,
            limits=limits,
            api=api_stats,
            domain=domain,
            tiers=tiers,
            reserved_memory=reserved_memory,
            reserved_storage=reserved_storage,
        )


@dataclass
class PublishAck:
    """Acknowledgement of a published message."""

    stream: str
    sequence: int | None = None
    domain: str | None = None
    duplicate: bool = False

    @classmethod
    def from_response(cls, data: api.PublishAck, *, strict: bool = False) -> PublishAck:
        stream = data.pop("stream")
        sequence = data.pop("seq", None)
        domain = data.pop("domain", None)
        duplicate = data.pop("duplicate", False)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"PublishAck.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            stream=stream,
            sequence=sequence,
            domain=domain,
            duplicate=duplicate,
        )


class JetStream:
    """JetStream context."""

    def __init__(self, client: Client, prefix: str = "$JS.API", domain: str | None = None) -> None:
        """Initialize JetStream client.

        Args:
            client: NATS client
            prefix: API prefix
            domain: JetStream domain
        """
        self._client = client
        if domain:
            prefix = f"$JS.{domain}.API"
        self._prefix = prefix
        self._api = api.Client(client, prefix)

    @property
    def client(self) -> Client:
        """Get the underlying NATS client."""
        return self._client

    @property
    def api_prefix(self) -> str:
        """Get the API prefix."""
        return self._prefix

    async def publish(
        self,
        subject: str,
        payload: bytes,
        *,
        headers: dict[str, str] | None = None,
    ) -> PublishAck:
        """Publish a message to JetStream.

        Args:
            subject: Subject to publish to
            payload: Message payload
            headers: Optional message headers

        Returns:
            Acknowledgement of the published message
        """
        response = await self._client.request(
            subject,
            payload,
            headers=headers,
            timeout=5.0,
        )

        publish_ack = PublishAck.from_response(json.loads(response.data))

        return publish_ack

    async def stream_names(self, subject: str | None = None) -> AsyncIterator[str]:
        """Get an async iterator over all stream names.

        Args:
            subject: Optional subject filter to match streams against

        Yields:
            Stream names one at a time.
        """
        offset = 0
        total = None
        while True:
            response = await self._api.stream_names(offset=offset, subject=subject)
            streams = response.get("streams", [])
            if streams is None:
                streams = []

            for name in streams:
                yield name

            # Update total if not set
            if total is None:
                total = response["total"]

            # Check if we've reached the end
            if not streams or offset + len(streams) >= total:
                break

            # Increment offset by the number of streams we received
            offset += len(streams)

    async def list_streams(self, subject: str | None = None) -> AsyncIterator[StreamInfo]:
        """Get an async iterator over all streams.

        Args:
            subject: Optional subject filter to match streams against

        Yields:
            StreamInfo objects one at a time.
        """
        offset = 0
        total = None
        while True:
            response = await self._api.stream_list(offset=offset, subject=subject)
            streams = response.get("streams", [])
            if streams is None:
                streams = []

            for stream in streams:
                yield StreamInfo.from_response(stream)

            # Update total if not set
            if total is None:
                total = response["total"]

            # Check if we've reached the end
            if not streams or offset + len(streams) >= total:
                break

            # Increment offset by the number of streams we received
            offset += len(streams)

    @overload
    async def create_stream(self, config: StreamConfig, /) -> Stream:
        """Create a new stream from a StreamConfig object."""
        ...

    @overload
    async def create_stream(self, *, name: str, **config) -> Stream:
        """Create a new stream with keyword arguments."""
        ...

    async def create_stream(self, config: StreamConfig | None = None, /, **kwargs) -> Stream:
        """Create a new stream.

        Args:
            config: A StreamConfig object to create the stream from (positional-only)
            **kwargs: Stream configuration parameters as keyword arguments

        Returns:
            The created Stream object
        """
        if config is None:
            # Create StreamConfig from kwargs with dict-to-dataclass conversion
            config = StreamConfig.from_kwargs(**kwargs)

        # Validate stream name
        if config.name is None:
            raise ValueError("StreamConfig must have a name")

        # Convert StreamConfig to API request format and create stream
        config_dict = config.to_request()
        response = await self._api.stream_create(**config_dict)
        info = StreamInfo.from_response(response)
        return Stream(self, config.name, info)

    async def update_stream(self, **config) -> StreamInfo:
        """Update an existing stream."""
        response = await self._api.stream_update(**config)
        return StreamInfo.from_response(response)

    async def delete_stream(self, name: str) -> bool:
        """Delete a stream."""
        response = await self._api.stream_delete(name)
        return response["success"]

    async def get_stream_info(
        self,
        name: str,
        *,
        deleted_details: bool = False,
        subjects_filter: str | None = None,
        offset: int | None = None,
    ) -> StreamInfo:
        """Get information about a stream."""
        response = await self._api.stream_info(
            name,
            deleted_details=deleted_details,
            subjects_filter=subjects_filter,
            offset=offset,
        )
        return StreamInfo.from_response(response)

    async def get_stream(self, name: str) -> Stream:
        """Get a stream by name."""
        info = await self.get_stream_info(name)
        return Stream(self, name, info)

    async def create_consumer(self, stream_name: str, name: str, durable_name: str | None = None, **config) -> Consumer:
        """Create a consumer for a stream.

        Args:
            stream_name: Name of the stream
            name: Name of the consumer
            durable_name: Optional durable name
            **config: Additional consumer configuration

        Returns:
            The created consumer
        """
        # Get the stream first
        stream = await self.get_stream(stream_name)

        # Prepare consumer configuration
        consumer_config = {"name": name}
        if durable_name:
            consumer_config["durable_name"] = durable_name
        consumer_config.update(config)

        # Create the consumer via the stream (using keyword arguments)
        return await stream.create_consumer(**consumer_config)

    async def get_consumer(self, stream_name: str, consumer_name: str) -> Consumer:
        """Get a consumer by name.

        Args:
            stream_name: Name of the stream
            consumer_name: Name of the consumer

        Returns:
            The consumer
        """
        stream = await self.get_stream(stream_name)
        return await stream.get_consumer(consumer_name)

    async def delete_consumer(self, stream_name: str, consumer_name: str) -> bool:
        """Delete a consumer.

        Args:
            stream_name: Name of the stream
            consumer_name: Name of the consumer

        Returns:
            True if the consumer was deleted
        """
        stream = await self.get_stream(stream_name)
        return await stream.delete_consumer(consumer_name)

    async def update_consumer(self, stream_name: str, consumer_name: str, **config) -> Consumer:
        """Update a consumer.

        Args:
            stream_name: Name of the stream
            consumer_name: Name of the consumer
            **config: New consumer configuration

        Returns:
            The updated consumer
        """
        stream = await self.get_stream(stream_name)
        return await stream.update_consumer(consumer_name, **config)

    async def consumer_names(self, stream_name: str) -> AsyncIterator[str]:
        """Get an async iterator over all consumer names for a stream.

        Args:
            stream_name: Name of the stream

        Yields:
            Consumer names one at a time
        """
        offset = 0
        total = None

        while True:
            response = await self._api.consumer_names(stream_name, offset=offset)
            consumers = response.get("consumers", [])

            if consumers is None:
                consumers = []

            for name in consumers:
                yield name

            # Update total if not set
            if total is None:
                total = response["total"]

            # Check if we've reached the end
            if not consumers or offset + len(consumers) >= total:
                break

            # Increment offset
            offset += len(consumers)

    async def list_consumers(self, stream_name: str) -> AsyncIterator[ConsumerInfo]:
        """Get an async iterator over all consumer info objects for a stream.

        Args:
            stream_name: Name of the stream

        Yields:
            ConsumerInfo objects one at a time
        """
        offset = 0
        total = None

        while True:
            response = await self._api.consumer_list(stream_name, offset=offset)
            consumers = response.get("consumers", [])

            if consumers is None:
                consumers = []

            for consumer in consumers:
                yield ConsumerInfo.from_response(consumer)

            # Update total if not set
            if total is None:
                total = response["total"]

            # Check if we've reached the end
            if not consumers or offset + len(consumers) >= total:
                break

            # Increment offset
            offset += len(consumers)

    async def get_consumer_info(self, stream_name: str, consumer_name: str) -> ConsumerInfo:
        """Get consumer info.

        Args:
            stream_name: Name of the stream
            consumer_name: Name of the consumer

        Returns:
            Consumer information
        """
        response = await self._api.consumer_info(stream_name, consumer_name)
        return ConsumerInfo.from_response(response)

    async def account_info(self) -> AccountInfo:
        """Get account information."""
        response = await self._api.account_info()
        return AccountInfo.from_response(response)

    async def get_message(self, stream: str, sequence: int) -> StreamMessage:
        """Get a message directly from a stream by sequence number.

        This is a direct message get that requires the stream to have allow_direct=true.
        For streams without direct access enabled, use Stream.get_message instead.

        Args:
            stream: Name of the stream to get the message from
            sequence: The sequence number of the message to get

        Returns:
            The stream message including subject, data, headers, etc.
        """
        response = await self._api.stream_msg_get(stream, seq=sequence)
        message = response["message"]

        # Decode base64 data if present
        data = None
        if "data" in message:
            data = base64.b64decode(message["data"])

        # Decode base64 headers if present
        headers = None
        if "hdrs" in message:
            try:
                headers_bytes = base64.b64decode(message["hdrs"])
                # parse_headers returns dict[str, list[str]]
                parsed_headers, _status_code, _status_description = parse_headers(headers_bytes)
                if parsed_headers:
                    headers = Headers(parsed_headers)
            except Exception:
                # If we can't parse headers, just set to None
                headers = None

        return StreamMessage(
            subject=message["subject"],
            sequence=message["seq"],
            data=data or b"",
            time=datetime.fromisoformat(message["time"].replace("Z", "+00:00")),
            headers=headers,
        )

    async def get_last_message_for_subject(self, stream: str, subject: str) -> StreamMessage:
        """Get the last message for a subject directly from a stream.

        This is a direct message get that requires the stream to have allow_direct=true.
        For streams without direct access enabled, use Stream.get_last_message_for_subject instead.

        Args:
            stream: Name of the stream to get the message from
            subject: The subject to get the last message for

        Returns:
            The stream message including subject, data, headers, etc.
        """
        response = await self._api.stream_msg_get(stream, last_by_subj=subject)
        message = response["message"]

        # Decode base64 data if present
        data = None
        if "data" in message:
            data = base64.b64decode(message["data"])

        # Decode base64 headers if present
        headers = None
        if "hdrs" in message:
            try:
                headers_bytes = base64.b64decode(message["hdrs"])
                # parse_headers returns dict[str, list[str]]
                parsed_headers, _status_code, _status_description = parse_headers(headers_bytes)
                if parsed_headers:
                    headers = Headers(parsed_headers)
            except Exception:
                # If we can't parse headers, just set to None
                headers = None

        return StreamMessage(
            subject=message["subject"],
            sequence=message["seq"],
            data=data or b"",
            time=datetime.fromisoformat(message["time"].replace("Z", "+00:00")),
            headers=headers,
        )


def new(client: Client, prefix: str = "$JS.API", domain: str | None = None) -> JetStream:
    """Create a new JetStream instance.

    Args:
        client: NATS client
        prefix: API prefix
        domain: JetStream domain

    Returns:
        A new JetStream instance
    """
    return JetStream(client, prefix, domain)


__all__ = [
    "JetStream",
    "Consumer",
    "ConsumerInfo",
    "Stream",
    "StreamConfig",
    "StreamConsumerLimits",
    "StreamInfo",
    "StreamMessage",
    "StreamSource",
    "StreamSourceInfo",
    "StreamState",
    "ClusterInfo",
    "PeerInfo",
    "Placement",
    "Republish",
    "ExternalStreamSource",
    "SubjectTransform",
    "AccountInfo",
    "AccountLimits",
    "Tier",
    "APIStats",
    "PublishAck",
]
