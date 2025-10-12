"""Consumer protocols and types for JetStream."""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Literal,
    Protocol,
    overload,
)

# Type aliases for consumer configuration enums
AckPolicy = Literal["none", "all", "explicit"]
DeliverPolicy = Literal["all", "last", "new", "by_start_sequence", "by_start_time", "last_per_subject"]
ReplayPolicy = Literal["instant", "original"]

if TYPE_CHECKING:
    from .. import api
    from ..message import Message


@dataclass
class ConsumerConfig:
    """Configuration for a JetStream consumer."""

    ack_policy: AckPolicy = "none"
    """The requirement of client acknowledgments."""

    deliver_policy: DeliverPolicy = "all"
    """The point in the stream from which to receive messages."""

    replay_policy: ReplayPolicy = "instant"
    """The rate at which messages will be pushed to a client."""

    ack_wait: int | None = None
    """How long (in nanoseconds) to allow messages to remain un-acknowledged before attempting redelivery."""

    backoff: list[int] | None = None
    """List of durations in Go format that represents a retry time scale for NaK'd messages."""

    deliver_group: str | None = None
    """The queue group name used to distribute messages among subscribers."""

    deliver_subject: str | None = None
    """The subject push consumers delivery messages to."""

    description: str | None = None
    """A short description of the purpose of this consumer."""

    direct: bool | None = None
    """Creates a special consumer that does not touch the Raft layers, not for general use by clients, internal use only."""

    durable_name: str | None = None
    """A unique name for a durable consumer (deprecated)."""

    filter_subject: str | None = None
    """Filter the stream by a single subject."""

    filter_subjects: list[str] | None = None
    """Filter the stream by multiple subjects."""

    flow_control: bool | None = None
    """For push consumers this will regularly send an empty message with Status header 100 and a reply subject, consumers must reply to these messages to control the rate of message delivery."""

    headers_only: bool | None = None
    """Delivers only the headers of messages in the stream and not the bodies. Additionally adds Nats-Msg-Size header to indicate the size of the removed payload."""

    idle_heartbeat: int | None = None
    """If the Consumer is idle for more than this many nano seconds a empty message with Status header 100 will be sent indicating the consumer is still alive."""

    inactive_threshold: int | None = None
    """Duration that instructs the server to cleanup ephemeral consumers that are inactive for that long."""

    max_ack_pending: int | None = None
    """The maximum number of messages without acknowledgement that can be outstanding, once this limit is reached message delivery will be suspended."""

    max_batch: int | None = None
    """The largest batch property that may be specified when doing a pull on a Pull Consumer."""

    max_bytes: int | None = None
    """The maximum bytes value that maybe set when doing a pull on a Pull Consumer."""

    max_deliver: int | None = None
    """The number of times a message will be delivered to consumers if not acknowledged in time."""

    max_expires: int | None = None
    """The maximum expires value that may be set when doing a pull on a Pull Consumer."""

    max_waiting: int | None = None
    """The number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored."""

    mem_storage: bool | None = None
    """Force the consumer state to be kept in memory rather than inherit the setting from the stream."""

    metadata: dict[str, str] | None = None
    """Additional metadata for the Consumer."""

    name: str | None = None
    """A unique name for a consumer."""

    num_replicas: int | None = None
    """When set do not inherit the replica count from the stream but specifically set it to this amount."""

    opt_start_seq: int | None = None
    """Start sequence used with the DeliverByStartSequence deliver policy."""

    opt_start_time: int | None = None
    """Start time used with the DeliverByStartTime deliver policy."""

    pause_until: int | None = None
    """When creating a consumer supplying a time in the future will act as a deadline for when the consumer will be paused till."""

    priority_groups: list[str] | None = None
    """List of priority groups this consumer supports."""

    priority_policy: str | None = None
    """The priority policy the consumer is set to."""

    priority_timeout: Any | None = None
    """For pinned_client priority policy how long before the client times out."""

    rate_limit_bps: int | None = None
    """The rate at which messages will be delivered to clients, expressed in bit per second."""

    sample_freq: str | None = None
    """Sets the percentage of acknowledgments that should be sampled for observability."""

    @classmethod
    def from_response(cls, config: api.ConsumerConfig, *, strict: bool = False) -> ConsumerConfig:
        """Create a ConsumerConfig from an API response."""
        # Pop fields as we consume them to detect unconsumed ones at the end
        ack_policy = config.pop("ack_policy", "none")
        deliver_policy = config.pop("deliver_policy", "all")
        replay_policy = config.pop("replay_policy", "instant")
        ack_wait = config.pop("ack_wait", None)
        backoff = config.pop("backoff", None)
        deliver_group = config.pop("deliver_group", None)
        deliver_subject = config.pop("deliver_subject", None)
        description = config.pop("description", None)
        direct = config.pop("direct", None)
        durable_name = config.pop("durable_name", None)
        filter_subject = config.pop("filter_subject", None)
        filter_subjects = config.pop("filter_subjects", None)
        flow_control = config.pop("flow_control", None)
        headers_only = config.pop("headers_only", None)
        idle_heartbeat = config.pop("idle_heartbeat", None)
        inactive_threshold = config.pop("inactive_threshold", None)
        max_ack_pending = config.pop("max_ack_pending", None)
        max_batch = config.pop("max_batch", None)
        max_bytes = config.pop("max_bytes", None)
        max_deliver = config.pop("max_deliver", None)
        max_expires = config.pop("max_expires", None)
        max_waiting = config.pop("max_waiting", None)
        mem_storage = config.pop("mem_storage", None)
        metadata = config.pop("metadata", None)
        name = config.pop("name", None)
        num_replicas = config.pop("num_replicas", None)
        opt_start_seq = config.pop("opt_start_seq", None)
        opt_start_time = config.pop("opt_start_time", None)
        pause_until = config.pop("pause_until", None)
        priority_groups = config.pop("priority_groups", None)
        priority_policy = config.pop("priority_policy", None)
        priority_timeout = config.pop("priority_timeout", None)
        rate_limit_bps = config.pop("rate_limit_bps", None)
        sample_freq = config.pop("sample_freq", None)

        # Check for unconsumed fields
        if strict and config:
            raise ValueError(f"ConsumerConfig.from_response() has unconsumed fields: {list(config.keys())}")

        return cls(
            ack_policy=ack_policy,
            deliver_policy=deliver_policy,
            replay_policy=replay_policy,
            ack_wait=ack_wait,
            backoff=backoff,
            deliver_group=deliver_group,
            deliver_subject=deliver_subject,
            description=description,
            direct=direct,
            durable_name=durable_name,
            filter_subject=filter_subject,
            filter_subjects=filter_subjects,
            flow_control=flow_control,
            headers_only=headers_only,
            idle_heartbeat=idle_heartbeat,
            inactive_threshold=inactive_threshold,
            max_ack_pending=max_ack_pending,
            max_batch=max_batch,
            max_bytes=max_bytes,
            max_deliver=max_deliver,
            max_expires=max_expires,
            max_waiting=max_waiting,
            mem_storage=mem_storage,
            metadata=metadata,
            name=name,
            num_replicas=num_replicas,
            opt_start_seq=opt_start_seq,
            opt_start_time=opt_start_time,
            pause_until=pause_until,
            priority_groups=priority_groups,
            priority_policy=priority_policy,
            priority_timeout=priority_timeout,
            rate_limit_bps=rate_limit_bps,
            sample_freq=sample_freq,
        )

    def to_request(self) -> dict[str, Any]:
        """Convert to API request format."""
        request: dict[str, Any] = {}

        # Add non-None fields to request
        if self.ack_policy != "none":
            request["ack_policy"] = self.ack_policy
        if self.deliver_policy != "all":
            request["deliver_policy"] = self.deliver_policy
        if self.replay_policy != "instant":
            request["replay_policy"] = self.replay_policy
        if self.ack_wait is not None:
            request["ack_wait"] = self.ack_wait
        if self.backoff is not None:
            request["backoff"] = self.backoff
        if self.deliver_group is not None:
            request["deliver_group"] = self.deliver_group
        if self.deliver_subject is not None:
            request["deliver_subject"] = self.deliver_subject
        if self.description is not None:
            request["description"] = self.description
        if self.direct is not None:
            request["direct"] = self.direct
        if self.durable_name is not None:
            request["durable_name"] = self.durable_name
        if self.filter_subject is not None:
            request["filter_subject"] = self.filter_subject
        if self.filter_subjects is not None:
            request["filter_subjects"] = self.filter_subjects
        if self.flow_control is not None:
            request["flow_control"] = self.flow_control
        if self.headers_only is not None:
            request["headers_only"] = self.headers_only
        if self.idle_heartbeat is not None:
            request["idle_heartbeat"] = self.idle_heartbeat
        if self.inactive_threshold is not None:
            request["inactive_threshold"] = self.inactive_threshold
        if self.max_ack_pending is not None:
            request["max_ack_pending"] = self.max_ack_pending
        if self.max_batch is not None:
            request["max_batch"] = self.max_batch
        if self.max_bytes is not None:
            request["max_bytes"] = self.max_bytes
        if self.max_deliver is not None:
            request["max_deliver"] = self.max_deliver
        if self.max_expires is not None:
            request["max_expires"] = self.max_expires
        if self.max_waiting is not None:
            request["max_waiting"] = self.max_waiting
        if self.mem_storage is not None:
            request["mem_storage"] = self.mem_storage
        if self.metadata is not None:
            request["metadata"] = self.metadata
        if self.name is not None:
            request["name"] = self.name
        if self.num_replicas is not None:
            request["num_replicas"] = self.num_replicas
        if self.opt_start_seq is not None:
            request["opt_start_seq"] = self.opt_start_seq
        if self.opt_start_time is not None:
            request["opt_start_time"] = self.opt_start_time
        if self.pause_until is not None:
            request["pause_until"] = self.pause_until
        if self.priority_groups is not None:
            request["priority_groups"] = self.priority_groups
        if self.priority_policy is not None:
            request["priority_policy"] = self.priority_policy
        if self.priority_timeout is not None:
            request["priority_timeout"] = self.priority_timeout
        if self.rate_limit_bps is not None:
            request["rate_limit_bps"] = self.rate_limit_bps
        if self.sample_freq is not None:
            request["sample_freq"] = self.sample_freq

        return request

    @classmethod
    def from_kwargs(cls, **kwargs) -> ConsumerConfig:
        """Create a ConsumerConfig from keyword arguments.

        This is a simple pass-through since ConsumerConfig has no nested
        dataclass fields that need conversion.
        """
        return cls(**kwargs)


@dataclass
class ConsumerInfo:
    """Information about a consumer."""

    stream_name: str
    name: str
    config: ConsumerConfig
    created: int
    delivered: dict[str, int]
    ack_floor: dict[str, int]
    num_ack_pending: int
    num_redelivered: int
    num_waiting: int
    num_pending: int
    cluster: dict[str, Any] | None = None
    push_bound: bool | None = None
    ts: int | None = None
    """The server time the consumer info was created."""

    @classmethod
    def from_response(cls, data: api.ConsumerInfo, *, strict: bool = False) -> ConsumerInfo:
        stream_name = data.pop("stream_name")
        name = data.pop("name")
        config_data = data.pop("config")
        config = ConsumerConfig.from_response(config_data, strict=strict)
        created = data.pop("created")
        delivered = data.pop("delivered")
        ack_floor = data.pop("ack_floor")
        num_ack_pending = data.pop("num_ack_pending")
        num_redelivered = data.pop("num_redelivered")
        num_waiting = data.pop("num_waiting")
        num_pending = data.pop("num_pending")
        cluster = data.pop("cluster", None)
        push_bound = data.pop("push_bound", None)
        ts = data.pop("ts", None)

        # Pop response envelope fields that aren't part of ConsumerInfo
        data.pop("type", None)  # Response type discriminator

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"ConsumerInfo.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            stream_name=stream_name,
            name=name,
            config=config,
            created=created,
            delivered=delivered,
            ack_floor=ack_floor,
            num_ack_pending=num_ack_pending,
            num_redelivered=num_redelivered,
            num_waiting=num_waiting,
            num_pending=num_pending,
            cluster=cluster,
            push_bound=push_bound,
            ts=ts,
        )


class MessageBatch(Protocol):
    """Protocol for a batch of messages retrieved from a JetStream consumer."""

    def __aiter__(self) -> AsyncIterator[Message]:
        """Return self as an async iterator."""
        ...

    async def __anext__(self) -> Message:
        """Get the next message from the batch."""
        ...


class MessageStream(Protocol):
    """Protocol for a continuous stream of messages from a JetStream consumer."""

    def __aiter__(self) -> AsyncIterator[Message]:
        """Return self as an async iterator."""
        ...

    async def __anext__(self) -> Message:
        """Get the next message from the stream."""
        ...


class Consumer(Protocol):
    """Protocol for a JetStream consumer."""

    @property
    def name(self) -> str:
        """Get the consumer name."""
        ...

    @property
    def stream_name(self) -> str:
        """Get the stream name."""
        ...

    @property
    def info(self) -> ConsumerInfo:
        """Get cached consumer info."""
        ...

    async def get_info(self) -> ConsumerInfo:
        """Get consumer info from the server."""
        ...

    @overload
    async def fetch(
        self,
        max_messages: int,
        max_wait: float | None = None,
        heartbeat: float | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer."""
        ...

    @overload
    async def fetch(
        self,
        *,
        max_wait: float | None = None,
        max_bytes: int | None = None,
        heartbeat: float | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer."""
        ...

    @overload
    async def fetch_nowait(
        self,
        *,
        max_messages: int | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer without waiting."""
        ...

    @overload
    async def fetch_nowait(
        self,
        *,
        max_bytes: int | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer without waiting."""
        ...

    async def next(self, max_wait: float = 5.0) -> Message:
        """Fetch a single message from the consumer."""
        ...

    async def messages(
        self,
        *,
        heartbeat: float | None = None,
        max_wait: float | None = None,
        max_messages: int | None = None,
        max_bytes: int | None = None,
    ) -> AsyncIterator[Message]:
        """Get an async iterator for continuous message consumption."""
        ...
