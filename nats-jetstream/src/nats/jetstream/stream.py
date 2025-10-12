"""Stream management for JetStream."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Literal,
    Protocol,
    overload,
    runtime_checkable,
)

from nats.client.message import Headers

from .consumer import Consumer, ConsumerConfig, ConsumerInfo
from .consumer.pull import PullConsumer

CONSUMER_ACTION_CREATE = "create"
CONSUMER_ACTION_UPDATE = "update"
CONSUMER_ACTION_CREATE_OR_UPDATE = ""

# Type aliases for stream configuration enums
RetentionPolicy = Literal["limits", "interest", "workqueue"]
StorageType = Literal["file", "memory"]
DiscardPolicy = Literal["old", "new"]
CompressionType = Literal["none", "s2"]

if TYPE_CHECKING:
    from . import JetStream, api


@dataclass
class LostStreamData:
    """Records messages that were damaged and unrecoverable."""

    msgs: list[int] | None = None
    """The messages that were lost."""

    bytes: int | None = None
    """The number of bytes that were lost."""

    @classmethod
    def from_response(cls, data: api.LostStreamData, *, strict: bool = False) -> LostStreamData:
        msgs = data.pop("msgs", None)
        bytes_val = data.pop("bytes", None)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"LostStreamData.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            msgs=msgs,
            bytes=bytes_val,
        )


@dataclass
class ExternalStreamSource:
    """Configuration referencing a stream source in another account or JetStream domain."""

    api: str
    """The subject prefix that imports the other account/domain $JS.API.CONSUMER.> subjects."""

    deliver: str | None = None
    """The delivery subject to use for the push consumer."""

    @classmethod
    def from_response(cls, data: api.ExternalStreamSource, *, strict: bool = False) -> ExternalStreamSource:
        api_prefix = data.pop("api")
        deliver = data.pop("deliver", None)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"ExternalStreamSource.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            api=api_prefix,
            deliver=deliver,
        )

    def to_request(self) -> api.ExternalStreamSource:
        """Convert to API request format."""
        result: api.ExternalStreamSource = {"api": self.api}
        if self.deliver is not None:
            result["deliver"] = self.deliver
        return result


@dataclass
class SubjectTransform:
    """Subject transform to apply to matching messages going into the stream."""

    src: str
    """The subject transform source."""

    dest: str
    """The subject transform destination."""

    @classmethod
    def from_response(cls, data: api.SubjectTransform, *, strict: bool = False) -> SubjectTransform:
        src = data.pop("src")
        dest = data.pop("dest")

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"SubjectTransform.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            src=src,
            dest=dest,
        )

    def to_request(self) -> api.SubjectTransform:
        """Convert to API request format."""
        result: api.SubjectTransform = {"src": self.src, "dest": self.dest}
        return result


@dataclass
class StreamSource:
    """Defines a source where streams should be replicated from."""

    name: str
    """Stream name."""

    opt_start_seq: int | None = None
    """Sequence to start replicating from."""

    opt_start_time: int | None = None
    """Time stamp to start replicating from."""

    filter_subject: str | None = None
    """Replicate only a subset of messages based on filter."""

    external: ExternalStreamSource | None = None
    """Configuration referencing a stream source in another account or JetStream domain."""

    subject_transforms: Any | None = None
    """The subject filtering sources and associated destination transforms."""

    @classmethod
    def from_response(cls, data: api.StreamSource, *, strict: bool = False) -> StreamSource:
        name = data.pop("name")
        opt_start_seq = data.pop("opt_start_seq", None)
        opt_start_time = data.pop("opt_start_time", None)
        filter_subject = data.pop("filter_subject", None)
        subject_transforms = data.pop("subject_transforms", None)

        external = None
        external_data = data.pop("external", None)
        if external_data:
            external = ExternalStreamSource.from_response(external_data, strict=strict)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"StreamSource.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            name=name,
            opt_start_seq=opt_start_seq,
            opt_start_time=opt_start_time,
            filter_subject=filter_subject,
            external=external,
            subject_transforms=subject_transforms,
        )

    def to_request(self) -> api.StreamSource:
        """Convert to API request format."""
        result: api.StreamSource = {"name": self.name}
        if self.opt_start_seq is not None:
            result["opt_start_seq"] = self.opt_start_seq
        if self.opt_start_time is not None:
            result["opt_start_time"] = self.opt_start_time
        if self.filter_subject is not None:
            result["filter_subject"] = self.filter_subject
        if self.external is not None:
            result["external"] = self.external.to_request()
        if self.subject_transforms is not None:
            result["subject_transforms"] = self.subject_transforms
        return result


@dataclass
class StreamSourceInfo:
    """Information about an upstream stream source in a mirror."""

    name: str
    """The name of the Stream being replicated."""

    lag: int
    """How many messages behind the mirror operation is."""

    active: int
    """When last the mirror had activity, in nanoseconds. Value will be -1 when there has been no activity."""

    filter_subject: str | None = None
    """The subject filter to apply to the messages."""

    external: ExternalStreamSource | None = None

    error: dict[str, Any] | None = None

    subject_transforms: Any | None = None
    """The subject filtering sources and associated destination transforms."""

    @classmethod
    def from_response(cls, data: api.StreamSourceInfo, *, strict: bool = False) -> StreamSourceInfo:
        name = data.pop("name")
        lag = data.pop("lag")
        active = data.pop("active")
        filter_subject = data.pop("filter_subject", None)
        subject_transforms = data.pop("subject_transforms", None)
        error = data.pop("error", None)

        external = None
        external_data = data.pop("external", None)
        if external_data:
            external = ExternalStreamSource.from_response(external_data, strict=strict)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"StreamSourceInfo.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            name=name,
            lag=lag,
            active=active,
            filter_subject=filter_subject,
            external=external,
            error=error,
            subject_transforms=subject_transforms,
        )


@dataclass
class Placement:
    """Placement requirements for a stream or asset leader."""

    cluster: str | None = None
    """The desired cluster name."""

    tags: list[str] | None = None
    """Tags required on servers hosting this stream or leader."""

    @classmethod
    def from_response(cls, data: api.Placement, *, strict: bool = False) -> Placement:
        cluster = data.pop("cluster", None)
        tags = data.pop("tags", None)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"Placement.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            cluster=cluster,
            tags=tags,
        )

    def to_request(self) -> api.Placement:
        """Convert to API request format."""
        result: api.Placement = {}
        if self.cluster is not None:
            result["cluster"] = self.cluster
        if self.tags is not None:
            result["tags"] = self.tags
        return result


@dataclass
class Republish:
    """Rules for republishing messages from a stream with subject mapping onto new subjects for partitioning and more."""

    src: str
    """The source subject to republish."""

    dest: str
    """The destination to publish to."""

    headers_only: bool | None = None
    """Only send message headers, no bodies."""

    @classmethod
    def from_response(cls, data: api.Republish, *, strict: bool = False) -> Republish:
        src = data.pop("src")
        dest = data.pop("dest")
        headers_only = data.pop("headers_only", None)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"Republish.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            src=src,
            dest=dest,
            headers_only=headers_only,
        )

    def to_request(self) -> api.Republish:
        """Convert to API request format."""
        result: api.Republish = {"src": self.src, "dest": self.dest}
        if self.headers_only is not None:
            result["headers_only"] = self.headers_only
        return result


@dataclass
class StreamConsumerLimits:
    """Limits for consumers of this stream."""

    inactive_threshold: int | None = None
    """Maximum value for inactive_threshold for consumers of this stream. Acts as a default when consumers do not set this value."""

    max_ack_pending: int | None = None
    """Maximum value for max_ack_pending for consumers of this stream. Acts as a default when consumers do not set this value."""

    @classmethod
    def from_response(cls, data: api.StreamConsumerLimits, *, strict: bool = False) -> StreamConsumerLimits:
        inactive_threshold = data.pop("inactive_threshold", None)
        max_ack_pending = data.pop("max_ack_pending", None)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"StreamConsumerLimits.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            inactive_threshold=inactive_threshold,
            max_ack_pending=max_ack_pending,
        )

    def to_request(self) -> api.StreamConsumerLimits:
        """Convert to API request format."""
        result: api.StreamConsumerLimits = {}
        if self.inactive_threshold is not None:
            result["inactive_threshold"] = self.inactive_threshold
        if self.max_ack_pending is not None:
            result["max_ack_pending"] = self.max_ack_pending
        return result


@dataclass
class PeerInfo:
    """Information about a peer in the cluster."""

    name: str
    """The server name of the peer."""

    current: bool
    """Indicates if the server is up to date and synchronised."""

    active: float
    """Nanoseconds since this peer was last seen."""

    lag: int | None = None
    """How many uncommitted operations this peer is behind the leader."""

    offline: bool | None = None
    """Indicates the node is considered offline by the group."""

    observer: bool | None = None
    """Indicates if the server is running as an observer only."""

    @classmethod
    def from_response(cls, data: api.PeerInfo, *, strict: bool = False) -> PeerInfo:
        name = data.pop("name")
        current = data.pop("current")
        active = data.pop("active")
        lag = data.pop("lag", None)
        offline = data.pop("offline", None)
        observer = data.pop("observer", None)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"PeerInfo.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            name=name,
            current=current,
            active=active,
            lag=lag,
            offline=offline,
            observer=observer,
        )


@dataclass
class ClusterInfo:
    """Information about the cluster and RAFT group managing an asset."""

    name: str | None = None
    """The cluster name."""

    leader: str | None = None
    """The server name of the RAFT leader."""

    replicas: list[PeerInfo] | None = None
    """The members of the RAFT cluster."""

    raft_group: str | None = None
    """In clustered environments the name of the Raft group managing the asset."""

    @classmethod
    def from_response(cls, data: api.ClusterInfo, *, strict: bool = False) -> ClusterInfo:
        name = data.pop("name", None)
        leader = data.pop("leader", None)
        raft_group = data.pop("raft_group", None)

        replicas = None
        replicas_data = data.pop("replicas", None)
        if replicas_data:
            replicas = [PeerInfo.from_response(r, strict=strict) for r in replicas_data]

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"ClusterInfo.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            name=name,
            leader=leader,
            replicas=replicas,
            raft_group=raft_group,
        )


@dataclass
class StreamState:
    """Current state information about a stream."""

    messages: int
    """Number of messages stored in the Stream."""

    bytes: int
    """Combined size of all messages in the Stream."""

    first_sequence: int
    """Sequence number of the first message in the Stream."""

    last_sequence: int
    """Sequence number of the last message in the Stream."""

    consumer_count: int
    """Number of Consumers attached to the Stream."""

    first_ts: str | None = None
    """The timestamp of the first message in the Stream."""

    last_ts: str | None = None
    """The timestamp of the last message in the Stream."""

    deleted: list[int] | None = None
    """IDs of messages that were deleted using the Message Delete API or Interest based streams removing messages out of order."""

    num_deleted: int | None = None
    """The number of deleted messages."""

    lost: LostStreamData | None = None

    subjects: dict[str, int] | None = None
    """Subjects and their message counts when a subjects_filter was set."""

    num_subjects: int | None = None
    """The number of unique subjects in the Stream."""

    @classmethod
    def from_response(cls, data: api.StreamState, *, strict: bool = False) -> StreamState:
        messages = data.pop("messages")
        bytes_val = data.pop("bytes")
        first_sequence = data.pop("first_seq")
        last_sequence = data.pop("last_seq")
        consumer_count = data.pop("consumer_count")
        first_ts = data.pop("first_ts", None)
        last_ts = data.pop("last_ts", None)
        deleted = data.pop("deleted", None)
        num_deleted = data.pop("num_deleted", None)
        subjects = data.pop("subjects", None)
        num_subjects = data.pop("num_subjects", None)

        lost = None
        lost_data = data.pop("lost", None)
        if lost_data:
            lost = LostStreamData.from_response(lost_data, strict=strict)

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"StreamState.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            messages=messages,
            bytes=bytes_val,
            first_sequence=first_sequence,
            last_sequence=last_sequence,
            consumer_count=consumer_count,
            first_ts=first_ts,
            last_ts=last_ts,
            deleted=deleted,
            num_deleted=num_deleted,
            lost=lost,
            subjects=subjects,
            num_subjects=num_subjects,
        )


@dataclass
class StreamConfig:
    """Configuration for a JetStream stream."""

    num_replicas: int = 1
    """How many replicas to keep for each message."""

    retention: RetentionPolicy = "limits"
    """How messages are retained in the Stream, once this is exceeded old messages are removed."""

    storage: StorageType = "file"
    """The storage backend to use for the Stream."""

    max_age: int | None = None
    """Maximum age of any message in the stream, expressed in nanoseconds. None for unlimited."""

    max_bytes: int | None = None
    """How big the Stream may be, when the combined stream size exceeds this old messages are removed. None for unlimited."""

    max_consumers: int | None = None
    """How many Consumers can be defined for a given Stream. None for unlimited."""

    max_msgs: int | None = None
    """How many messages may be in a Stream, oldest messages will be removed if the Stream exceeds this size. None for unlimited."""

    allow_direct: bool | None = None
    """Allow higher performance, direct access to get individual messages."""

    allow_msg_ttl: bool | None = None
    """Enables per-message TTL using headers."""

    allow_rollup_hdrs: bool | None = None
    """Allows the use of the Nats-Rollup header to replace all contents of a stream, or subject in a stream, with a single new message."""

    compression: CompressionType | None = None
    """Optional compression algorithm used for the Stream."""

    deny_delete: bool | None = None
    """Restricts the ability to delete messages from a stream via the API. Cannot be changed once set to true."""

    deny_purge: bool | None = None
    """Restricts the ability to purge messages from a stream via the API. Cannot be change once set to true."""

    description: str | None = None
    """A short description of the purpose of this stream."""

    discard: DiscardPolicy | None = None
    """When a Stream reach it's limits either old messages are deleted or new ones are denied."""

    discard_new_per_subject: bool | None = None
    """When discard policy is new and the stream is one with max messages per subject set, this will apply the new behavior to every subject. Essentially turning discard new from maximum number of subjects into maximum number of messages in a subject."""

    duplicate_window: int | None = None
    """The time window to track duplicate messages for, expressed in nanoseconds. 0 for default."""

    first_seq: int | None = None
    """A custom sequence to use for the first message in the stream."""

    max_msg_size: int | None = None
    """The largest message that will be accepted by the Stream. -1 for unlimited."""

    max_msgs_per_subject: int | None = None
    """For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit."""

    metadata: dict[str, str] | None = None
    """Additional metadata for the Stream."""

    mirror: StreamSource | None = None
    """Maintains a 1:1 mirror of another stream with name matching this property. When a mirror is configured subjects and sources must be empty."""

    mirror_direct: bool | None = None
    """Allow higher performance, direct access for mirrors as well."""

    name: str | None = None
    """A unique name for the Stream, empty for Stream Templates."""

    no_ack: bool | None = None
    """Disables acknowledging messages that are received by the Stream."""

    placement: Placement | None = None
    """Placement directives to consider when placing replicas of this stream, random placement when unset."""

    republish: Republish | None = None

    sealed: bool | None = None
    """Sealed streams do not allow messages to be deleted via limits or API, sealed streams can not be unsealed via configuration update. Can only be set on already created streams via the Update API."""

    sources: list[StreamSource] | None = None
    """List of Stream names to replicate into this Stream."""

    subject_delete_marker_ttl: int | None = None
    """Enables and sets a duration for adding server markers for delete, purge and max age limits."""

    subject_transform: SubjectTransform | None = None
    """Subject transform to apply to matching messages."""

    subjects: list[str] | None = None
    """A list of subjects to consume, supports wildcards. Must be empty when a mirror is configured. May be empty when sources are configured."""

    template_owner: str | None = None
    """When the Stream is managed by a Stream Template this identifies the template that manages the Stream."""

    consumer_limits: StreamConsumerLimits | None = None
    """Limits of certain values that consumers can set, defaults for those who don't set these settings."""

    @classmethod
    def from_kwargs(cls, **kwargs) -> StreamConfig:
        """Create a StreamConfig from keyword arguments, converting dicts to dataclasses."""
        # Convert mirror dict to StreamSource (with nested conversion)
        if "mirror" in kwargs and isinstance(kwargs["mirror"], dict):
            mirror_dict = kwargs["mirror"].copy()
            if "external" in mirror_dict and isinstance(mirror_dict["external"], dict):
                mirror_dict["external"] = ExternalStreamSource(**mirror_dict["external"])
            kwargs["mirror"] = StreamSource(**mirror_dict)

        # Convert placement dict to Placement
        if "placement" in kwargs and isinstance(kwargs["placement"], dict):
            kwargs["placement"] = Placement(**kwargs["placement"])

        # Convert republish dict to Republish
        if "republish" in kwargs and isinstance(kwargs["republish"], dict):
            kwargs["republish"] = Republish(**kwargs["republish"])

        # Convert subject_transform dict to SubjectTransform
        if "subject_transform" in kwargs and isinstance(kwargs["subject_transform"], dict):
            kwargs["subject_transform"] = SubjectTransform(**kwargs["subject_transform"])

        # Convert consumer_limits dict to StreamConsumerLimits
        if "consumer_limits" in kwargs and isinstance(kwargs["consumer_limits"], dict):
            kwargs["consumer_limits"] = StreamConsumerLimits(**kwargs["consumer_limits"])

        # Convert sources list of dicts to list of StreamSource
        if "sources" in kwargs and kwargs["sources"] is not None:
            converted_sources = []
            for source in kwargs["sources"]:
                if isinstance(source, dict):
                    source_dict = source.copy()
                    if "external" in source_dict and isinstance(source_dict["external"], dict):
                        source_dict["external"] = ExternalStreamSource(**source_dict["external"])
                    converted_sources.append(StreamSource(**source_dict))
                else:
                    converted_sources.append(source)
            kwargs["sources"] = converted_sources

        return cls(**kwargs)

    @classmethod
    def from_response(cls, config: api.StreamConfig, *, strict: bool = False) -> StreamConfig:
        """Create a StreamConfig from an API response"""
        # Pop fields as we consume them to detect unconsumed ones at the end
        # No need to copy - config comes from JSON and won't be reused

        # Convert API values to None for unlimited (-1 or 0)
        max_age = config.pop("max_age", 0)
        max_age = None if max_age == 0 else max_age

        max_bytes = config.pop("max_bytes", -1)
        max_bytes = None if max_bytes == -1 else max_bytes

        max_consumers = config.pop("max_consumers", -1)
        max_consumers = None if max_consumers == -1 else max_consumers

        max_msgs = config.pop("max_msgs", -1)
        max_msgs = None if max_msgs == -1 else max_msgs

        num_replicas = config.pop("num_replicas", 1)
        retention = config.pop("retention", "limits")
        storage = config.pop("storage", "file")

        allow_direct = config.pop("allow_direct", None)
        allow_msg_ttl = config.pop("allow_msg_ttl", None)
        allow_rollup_hdrs = config.pop("allow_rollup_hdrs", None)
        compression = config.pop("compression", None)
        deny_delete = config.pop("deny_delete", None)
        deny_purge = config.pop("deny_purge", None)
        description = config.pop("description", None)
        discard = config.pop("discard", None)
        discard_new_per_subject = config.pop("discard_new_per_subject", None)
        duplicate_window = config.pop("duplicate_window", None)
        first_seq = config.pop("first_seq", None)

        max_msg_size = config.pop("max_msg_size", -1)
        max_msg_size = None if max_msg_size == -1 else max_msg_size

        max_msgs_per_subject = config.pop("max_msgs_per_subject", None)
        metadata = config.pop("metadata", None)
        mirror_direct = config.pop("mirror_direct", None)
        name = config.pop("name", None)
        no_ack = config.pop("no_ack", None)
        sealed = config.pop("sealed", None)
        subjects = config.pop("subjects", None)
        subject_delete_marker_ttl = config.pop("subject_delete_marker_ttl", None)
        template_owner = config.pop("template_owner", None)

        mirror = None
        mirror_data = config.pop("mirror", None)
        if mirror_data:
            mirror = StreamSource.from_response(mirror_data, strict=strict)

        sources = None
        sources_data = config.pop("sources", None)
        if sources_data:
            sources = [StreamSource.from_response(s, strict=strict) for s in sources_data]

        subject_transform = None
        subject_transform_data = config.pop("subject_transform", None)
        if subject_transform_data:
            subject_transform = SubjectTransform.from_response(subject_transform_data, strict=strict)

        placement = None
        placement_data = config.pop("placement", None)
        if placement_data:
            placement = Placement.from_response(placement_data, strict=strict)

        republish = None
        republish_data = config.pop("republish", None)
        if republish_data:
            republish = Republish.from_response(republish_data, strict=strict)

        consumer_limits = None
        consumer_limits_data = config.pop("consumer_limits", None)
        if consumer_limits_data:
            consumer_limits = StreamConsumerLimits.from_response(consumer_limits_data, strict=strict)

        # Check for unconsumed fields
        if strict and config:
            raise ValueError(f"StreamConfig.from_response() has unconsumed fields: {list(config.keys())}")

        return cls(
            max_age=max_age,
            max_bytes=max_bytes,
            max_consumers=max_consumers,
            max_msgs=max_msgs,
            num_replicas=num_replicas,
            retention=retention,
            storage=storage,
            allow_direct=allow_direct,
            allow_msg_ttl=allow_msg_ttl,
            allow_rollup_hdrs=allow_rollup_hdrs,
            compression=compression,
            deny_delete=deny_delete,
            deny_purge=deny_purge,
            description=description,
            discard=discard,
            discard_new_per_subject=discard_new_per_subject,
            duplicate_window=duplicate_window,
            first_seq=first_seq,
            max_msg_size=max_msg_size,
            max_msgs_per_subject=max_msgs_per_subject,
            metadata=metadata,
            mirror=mirror,
            mirror_direct=mirror_direct,
            name=name,
            no_ack=no_ack,
            placement=placement,
            republish=republish,
            sealed=sealed,
            sources=sources,
            subjects=subjects,
            subject_delete_marker_ttl=subject_delete_marker_ttl,
            subject_transform=subject_transform,
            template_owner=template_owner,
            consumer_limits=consumer_limits,
        )

    def to_request(self) -> api.StreamConfig:
        """Convert StreamConfig to an API request dictionary."""
        result: api.StreamConfig = {
            "max_age": self.max_age if self.max_age is not None else 0,
            "max_bytes": self.max_bytes if self.max_bytes is not None else -1,
            "max_consumers": self.max_consumers if self.max_consumers is not None else -1,
            "max_msgs": self.max_msgs if self.max_msgs is not None else -1,
            "max_msg_size": self.max_msg_size if self.max_msg_size is not None else -1,
            "num_replicas": self.num_replicas,
            "retention": self.retention,
            "storage": self.storage,
        }

        # Add optional fields only if not None
        if self.allow_direct is not None:
            result["allow_direct"] = self.allow_direct
        if self.allow_msg_ttl is not None:
            result["allow_msg_ttl"] = self.allow_msg_ttl
        if self.allow_rollup_hdrs is not None:
            result["allow_rollup_hdrs"] = self.allow_rollup_hdrs
        if self.compression is not None:
            result["compression"] = self.compression
        if self.deny_delete is not None:
            result["deny_delete"] = self.deny_delete
        if self.deny_purge is not None:
            result["deny_purge"] = self.deny_purge
        if self.description is not None:
            result["description"] = self.description
        if self.discard is not None:
            result["discard"] = self.discard
        if self.discard_new_per_subject is not None:
            result["discard_new_per_subject"] = self.discard_new_per_subject
        if self.duplicate_window is not None:
            result["duplicate_window"] = self.duplicate_window
        if self.first_seq is not None:
            result["first_seq"] = self.first_seq
        if self.max_msgs_per_subject is not None:
            result["max_msgs_per_subject"] = self.max_msgs_per_subject
        if self.metadata is not None:
            result["metadata"] = self.metadata
        if self.mirror is not None:
            result["mirror"] = self.mirror.to_request()
        if self.mirror_direct is not None:
            result["mirror_direct"] = self.mirror_direct
        if self.name is not None:
            result["name"] = self.name
        if self.no_ack is not None:
            result["no_ack"] = self.no_ack
        if self.placement is not None:
            result["placement"] = self.placement.to_request()
        if self.republish is not None:
            result["republish"] = self.republish.to_request()
        if self.sealed is not None:
            result["sealed"] = self.sealed
        if self.sources is not None:
            result["sources"] = [s.to_request() for s in self.sources]
        if self.subject_delete_marker_ttl is not None:
            result["subject_delete_marker_ttl"] = self.subject_delete_marker_ttl
        if self.subject_transform is not None:
            result["subject_transform"] = self.subject_transform.to_request()
        if self.subjects is not None:
            result["subjects"] = self.subjects
        if self.template_owner is not None:
            result["template_owner"] = self.template_owner
        if self.consumer_limits is not None:
            result["consumer_limits"] = self.consumer_limits.to_request()

        return result


@dataclass
class StreamInfo:
    """Information about a JetStream stream."""

    config: StreamConfig
    """The active configuration for the Stream."""

    created: int
    """Timestamp when the stream was created."""

    state: StreamState
    """Detail about the current State of the Stream."""

    cluster: ClusterInfo | None = None

    mirror: StreamSourceInfo | None = None

    sources: list[StreamSourceInfo] | None = None
    """Streams being sourced into this Stream."""

    ts: int | None = None
    """The server time the stream info was created."""

    @classmethod
    def from_response(cls, data: api.StreamInfo, *, strict: bool = False) -> StreamInfo:
        config = StreamConfig.from_response(data.pop("config"), strict=strict)
        created = data.pop("created")
        state = StreamState.from_response(data.pop("state"), strict=strict)
        ts = data.pop("ts", None)

        cluster = None
        cluster_data = data.pop("cluster", None)
        if cluster_data:
            cluster = ClusterInfo.from_response(cluster_data, strict=strict)

        mirror = None
        mirror_data = data.pop("mirror", None)
        if mirror_data:
            mirror = StreamSourceInfo.from_response(mirror_data, strict=strict)

        sources = None
        sources_data = data.pop("sources", None)
        if sources_data:
            sources = [StreamSourceInfo.from_response(s, strict=strict) for s in sources_data]

        # Pop response envelope fields that aren't part of StreamInfo
        data.pop("type", None)  # Response type discriminator
        data.pop("did_create", None)  # Create response specific field
        data.pop("alternates", None)  # TODO: Add this field to dataclass
        data.pop("limit", None)  # Pagination field from list responses
        data.pop("offset", None)  # Pagination field from list responses
        data.pop("total", None)  # Pagination field from list responses

        # Check for unconsumed fields
        if strict and data:
            raise ValueError(f"StreamInfo.from_response() has unconsumed fields: {list(data.keys())}")

        return cls(
            config=config,
            created=created,
            state=state,
            ts=ts,
            cluster=cluster,
            mirror=mirror,
            sources=sources,
        )


@runtime_checkable
class StreamManager(Protocol):
    """Protocol for managing JetStream streams."""

    async def stream_names(self) -> AsyncIterator[str]:
        """Get an async iterator over all stream names.

        Yields:
            Stream names one at a time.
        """
        ...

    async def list_streams(self, subject: str | None = None) -> AsyncIterator[StreamInfo]:
        """Get an async iterator over all streams.

        Args:
            subject: Optional subject filter to match streams against

        Yields:
            StreamInfo objects one at a time.
        """
        ...

    async def create_stream(self, **config) -> Stream:
        """Create a new stream."""
        ...

    async def update_stream(self, **config) -> StreamInfo:
        """Update an existing stream."""
        ...

    async def delete_stream(self, name: str) -> bool:
        """Delete a stream."""
        ...

    async def get_stream_info(
        self,
        name: str,
        *,
        deleted_details: bool = False,
        subjects_filter: str | None = None,
        offset: int | None = None,
    ) -> StreamInfo:
        """Get information about a stream."""
        ...

    async def get_stream(self, name: str) -> Stream:
        """Get a stream by name."""
        ...


@dataclass
class StreamMessage:
    """A message stored in a JetStream stream."""

    subject: str
    sequence: int
    data: bytes
    time: datetime
    headers: Headers | None = None

    def __getitem__(self, key):
        """Support dictionary-like access to message attributes."""
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(f"StreamMessage has no attribute '{key}'")


class Stream:
    """A JetStream stream."""

    def __init__(self, jetstream: JetStream, name: str, info: StreamInfo | None = None) -> None:
        self._jetstream = jetstream
        self._name = name
        self._info = info

    @property
    def name(self) -> str:
        """Get the stream name."""
        return self._name

    @property
    def info(self) -> StreamInfo | None:
        """Get cached stream info."""
        return self._info

    async def get_info(
        self,
        *,
        deleted_details: bool = True,
        subjects_filter: str | None = None,
        offset: int | None = None,
    ) -> StreamInfo:
        """Get stream info from the server."""
        info = await self._jetstream.get_stream_info(
            self._name,
            deleted_details=deleted_details,
            subjects_filter=subjects_filter,
            offset=offset,
        )
        self._info = info
        return info

    async def purge(
        self,
        *,
        filter: str | None = None,
        keep: int | None = None,
        sequence: int | None = None,
    ) -> None:
        """Purge messages from the stream.

        Args:
            filter: Optional subject filter to purge messages for
            keep: Optional number of messages to keep
            sequence: Optional sequence number to purge up to
        """
        # Get the API client from the manager
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # Build purge request
        request = {}
        if filter is not None:
            request["filter"] = filter
        if keep is not None:
            request["keep"] = keep
        if sequence is not None:
            request["seq"] = sequence

        # Send purge request
        await api.stream_purge(self._name, **request)

    async def _get_message(self, *, sequence: int | None = None, last_by_subject: str | None = None) -> StreamMessage:
        """Internal helper to get a message either by sequence or last by subject.

        Args:
            sequence: Optional sequence number to get
            last_by_subject: Optional subject to get last message for

        Returns:
            The stream message

        Raises:
            RuntimeError: If API client is not available
        """
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # handle direct gets
        if self._info and self._info.config.allow_direct:
            # Use direct message access endpoint
            if sequence is not None:
                response = await api._client.request(
                    f"$JS.API.DIRECT.GET.{self._name}",
                    json.dumps({"seq": sequence}).encode(),
                )
            else:
                # Must be last by subject
                response = await api._client.request(f"$JS.API.DIRECT.GET.{self._name}.{last_by_subject}", b"")

            # Direct get returns raw message with headers
            if not hasattr(response, "headers") or not response.headers:
                # If no headers at all, treat as empty headers
                return StreamMessage(
                    subject=last_by_subject or "",
                    sequence=sequence or 0,
                    data=response.data,
                    time=datetime.now(timezone.utc),
                    headers=None,
                )

            # Parse required headers
            stream = response.headers.get("Nats-Stream")
            if not stream:
                raise RuntimeError("Direct get response missing Nats-Stream header")

            subject = response.headers.get("Nats-Subject")
            if not subject:
                raise RuntimeError("Direct get response missing Nats-Subject header")

            sequence = response.headers.get("Nats-Sequence")
            if not sequence:
                raise RuntimeError("Direct get response missing Nats-Sequence header")

            timestamp = response.headers.get("Nats-Time-Stamp")
            if not timestamp:
                raise RuntimeError("Direct get response missing Nats-Time-Stamp header")

            # Extract user headers (excluding NATS metadata headers)
            user_headers = {}
            metadata_headers = {"Nats-Stream", "Nats-Subject", "Nats-Sequence", "Nats-Time-Stamp"}

            for key, value in response.headers.items():
                if key not in metadata_headers:
                    user_headers[key] = value

            # Create Headers object if there are user headers
            headers_obj = Headers(user_headers) if user_headers else None

            return StreamMessage(
                subject=subject[0] if isinstance(subject, list) else subject,
                sequence=int(sequence[0] if isinstance(sequence, list) else sequence),
                data=response.data,
                time=datetime.fromisoformat(timestamp[0] if isinstance(timestamp, list) else timestamp),
                headers=headers_obj,
            )
        else:
            # Use standard message get which returns:
            # {
            #   "message": {
            #     "subject": "...",
            #     "seq": N,
            #     "time": "...",
            #     "data": "..." (base64),
            #     "hdrs": "..." (base64, optional)
            #     "stream": "...",
            #     "domain": "..."
            #   },
            #   "type": "io.nats.jetstream.api.v1.stream_msg_get_response"
            # }
            response = await api.stream_msg_get(self._name, seq=sequence, last_by_subj=last_by_subject)
            message = response["message"]

        # Decode base64 data if present
        data = b""  # Initialize with empty bytes by default
        if "data" in message:
            data = base64.b64decode(message["data"])

        # Parse headers if present
        headers = None
        if "hdrs" in message:
            try:
                from nats.client.protocol.message import parse_headers

                headers_bytes = base64.b64decode(message["hdrs"])
                # parse_headers returns dict[str, list[str]], status_code, status_description
                parsed_headers, _status_code, _status_description = parse_headers(headers_bytes)
                if parsed_headers:
                    headers = Headers(parsed_headers)
            except Exception:
                # If we can't parse headers, just ignore them
                headers = None

        return StreamMessage(
            subject=message["subject"],
            sequence=message["seq"],
            data=data,
            time=datetime.fromisoformat(message["time"].replace("Z", "+00:00")),
            headers=headers,
        )

    async def get_message(self, sequence: int) -> StreamMessage:
        """Get a message by sequence number.

        If the stream has allow_direct=true configured, this will use direct message access.
        Otherwise, it will use the standard message get API.

        Args:
            sequence: The sequence number of the message to get

        Returns:
            The stream message including subject, data, headers, etc.
        """
        return await self._get_message(sequence=sequence)

    async def get_last_message_for_subject(self, subject: str) -> StreamMessage:
        """Get the last message for a subject.

        If the stream has allow_direct=true configured, this will use direct message access.
        Otherwise, it will use the standard message get API.

        Args:
            subject: The subject to get the last message for

        Returns:
            The stream message including subject, data, headers, etc.
        """
        return await self._get_message(last_by_subject=subject)

    async def delete_message(self, sequence: int) -> None:
        """Delete a message from the stream.

        This removes the message from the stream without secure erasure.
        For secure deletion that overwrites data, use secure_delete_message().

        Args:
            sequence: The sequence number of the message to delete
        """
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        await api.stream_msg_delete(self._name, seq=sequence, no_erase=True)

    async def secure_delete_message(self, sequence: int) -> None:
        """Securely delete a message from the stream.

        This removes the message and overwrites its data with random data
        for secure erasure. This is slower than regular deletion.

        Args:
            sequence: The sequence number of the message to delete
        """
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        await api.stream_msg_delete(self._name, seq=sequence, no_erase=False)

    async def _upsert_consumer(self, action: str = CONSUMER_ACTION_CREATE_OR_UPDATE, **config) -> Consumer:
        """Upsert a consumer for this stream, mirroring Go's upsertConsumer logic.

        Args:
            name: Name of the consumer
            action: Action to perform ("create" or "update")
            **config: Additional consumer configuration

        Returns:
            The created/updated consumer
        """
        # Get API client from jetstream
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        name = config.get("name")
        if name is None:
            durable_name = config.get("durable_name")
            if durable_name is None:
                name = f"consumer-{base64.b64encode(str(datetime.now(timezone.utc)).encode()).decode()}"
            else:
                name = durable_name

        request = {
            "action": action,
            "stream_name": self._name,
            "config": {
                **config,
                "name": name,
            },
        }

        # Create/update consumer via API
        response = await api.consumer_create(**request)
        consumer_info = ConsumerInfo.from_response(response)

        # Check if this is a push consumer (has deliver_subject)
        if consumer_info.config.deliver_subject:
            raise NotImplementedError("Push consumers are not supported yet")

        # Return new consumer instance using the actual name from the server response
        return PullConsumer(self, consumer_info)

    @overload
    async def create_consumer(self, config: ConsumerConfig, /) -> Consumer:
        """Create a consumer from a ConsumerConfig object."""
        ...

    @overload
    async def create_consumer(self, *, name: str, **config) -> Consumer:
        """Create a consumer with keyword arguments."""
        ...

    async def create_consumer(self, config: ConsumerConfig | None = None, /, **kwargs) -> Consumer:
        """Create a consumer for this stream.

        Args:
            config: A ConsumerConfig object to create the consumer from (positional-only)
            **kwargs: Consumer configuration parameters as keyword arguments

        Returns:
            The created consumer
        """
        # Get API client from jetstream
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        if config is None:
            # Create ConsumerConfig from kwargs
            config = ConsumerConfig.from_kwargs(**kwargs)

        # Validate consumer name
        if config.name is None:
            raise ValueError("ConsumerConfig must have a name")

        # Convert ConsumerConfig to API request format and create consumer
        config_dict = config.to_request()

        # Delegate to _upsert_consumer with "create" action
        return await self._upsert_consumer(action=CONSUMER_ACTION_CREATE, **config_dict)

    async def get_consumer_info(self, consumer_name: str) -> ConsumerInfo:
        """Get consumer info."""
        # Get API client from jetstream
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # Get consumer info via API
        response = await api.consumer_info(self._name, consumer_name)
        return ConsumerInfo.from_response(response)

    async def get_consumer(self, consumer_name: str) -> Consumer:
        """Get a consumer by name."""
        # Get consumer info
        consumer_info = await self.get_consumer_info(consumer_name)

        # Check if this is a push consumer (has deliver_subject)
        if consumer_info.config.deliver_subject:
            raise NotImplementedError("Push consumers are not supported yet")

        # Return new consumer instance
        return PullConsumer(self, consumer_info)

    async def delete_consumer(self, consumer_name: str) -> bool:
        """Delete a consumer."""
        # Get API client from jetstream
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # Delete consumer via API
        response = await api.consumer_delete(self._name, consumer_name)
        return response["success"]

    async def update_consumer(self, consumer_name: str, **config) -> Consumer:
        """Update a consumer.

        Args:
            consumer_name: Name of the consumer to update
            **config: New consumer configuration

        Returns:
            The updated consumer
        """
        return await self._upsert_consumer(action=CONSUMER_ACTION_UPDATE, name=consumer_name, **config)
