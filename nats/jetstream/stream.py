from __future__ import annotations

from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
from typing import AsyncIterable, Dict, Any, Optional, List, AsyncIterator, Protocol

from nats import jetstream

from .api import (
    STREAM_NAME_IN_USE,
    STREAM_NOT_FOUND,
    Client,
    JetStreamError,
    JetStreamError,
)

from .consumer import (
    ClusterInfo,
    ConsumerConfig,
    Consumer,
    _create_consumer,
    _delete_consumer,
    _get_consumer,
    _update_consumer,
    _create_or_update_consumer,
)

class RetentionPolicy(Enum):
    """
    Determines how messages in a stream are retained.
    """

    LIMITS = "limits"
    """LimitsPolicy means that messages are retained until any given limit is reached. This could be one of MaxMsgs, MaxBytes, or MaxAge."""

    INTEREST = "interest"
    """InterestPolicy specifies that when all known observables have acknowledged a message, it can be removed."""

    WORKQUEUE = "workqueue"
    """WorkQueuePolicy specifies that when the first worker or subscriber acknowledges the message, it can be removed."""


class DiscardPolicy(Enum):
    """
    Determines how to proceed when limits of messages or bytes are reached.
    """

    OLD = "old"
    """DiscardOld will remove older messages to return to the limits.

    This is the default.
    """

    NEW = "new"
    """DiscardNew will fail to store new messages once the limits are reached."""


class StorageType(Enum):
    """
    Determines how messages are stored for retention.
    """

    FILE = "file"
    """
    Specifies on disk storage.

    This is the default.
    """

    MEMORY = "memory"
    """
    Specifies in-memory storage.
    """


class StoreCompression(Enum):
    """
    Determines how messages are compressed.
    """

    NONE = "none"
    """
    Disables compression on the stream.

    This is the default.
    """

    S2 = "s2"
    """
    Enables S2 compression on the stream.
    """


@dataclass
class SubjectTransformConfig:
    """
    SubjectTransformConfig is for applying a subject transform (to matching
    messages) before doing anything else when a new message is received.
    """

    source: str
    """The subject pattern to match incoming messages against."""

    destination: str
    """The subject pattern to remap the subject to."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SubjectTransformConfig:
        return cls(source=data["src"], destination=data["dest"])

    def to_dict(self) -> Dict[str, str]:
        return {"src": self.source, "dest": self.destination}


@dataclass
class StreamSourceInfo:
    """
    StreamSourceInfo shows information about an upstream stream source/mirror.
    """

    name: str
    """Name is the name of the stream that is being replicated."""

    lag: Optional[int] = None
    """Lag informs how many messages behind the source/mirror operation is. This will only show correctly if there is active communication with stream/mirror."""

    active: Optional[int] = None
    """Active informs when last the mirror or sourced stream had activity. Value will be -1 when there has been no activity."""

    filter_subject: Optional[str] = None
    """FilterSubject is the subject filter defined for this source/mirror."""

    subject_transforms: List[SubjectTransformConfig] = field(default_factory=list)
    """SubjectTransforms is a list of subject transforms defined for this source/mirror."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StreamSourceInfo:
        return cls(
            name=data["name"],
            lag=data.get("lag", None),
            active=data.get("active", None),
            filter_subject=data.get("filter_subject", None),
            subject_transforms=[
                SubjectTransformConfig.from_dict(x)
                for x in data.get("subject_transforms", [])
            ],
        )


@dataclass
class StreamState:
    """
    StreamState is the state of a JetStream stream at the time of request.
    """

    msgs: int
    """The number of messages stored in the stream."""

    bytes: int
    """The number of bytes stored in the stream."""

    first_sequence: int
    """The the sequence number of the first message in the stream."""

    first_time: datetime
    """The timestamp of the first message in the stream."""

    last_sequence: int
    """The sequence number of the last message in the stream."""

    last_time: datetime
    """The timestamp of the last message in the stream."""

    consumers: int
    """The number of consumers on the stream."""

    num_deleted: int
    """NumDeleted is the number of messages that have been removed from the stream. Only deleted messages causing a gap in stream sequence numbers are counted. Messages deleted at the beginning or end of the stream are not counted."""

    num_subjects: int
    """NumSubjects is the number of unique subjects the stream has received messages on."""

    deleted: Optional[List[int]] = None
    """A list of sequence numbers that have been removed from the stream. This field will only be returned if the stream has been fetched with the DeletedDetails option."""

    subjects: Optional[Dict[str, int]] = None
    """Subjects is a map of subjects the stream has received messages on with message count per subject. This field will only be returned if the stream has been fetched with the SubjectFilter option."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StreamState:
        return cls(
            msgs=data["messages"],
            bytes=data["bytes"],
            first_sequence=data["first_seq"],
            first_time=datetime.strptime(data["first_ts"], "%Y-%m-%dT%H:%M:%SZ"),
            last_sequence=data["last_seq"],
            last_time=datetime.strptime(data["last_ts"], "%Y-%m-%dT%H:%M:%SZ"),
            consumers=data["consumer_count"],
            num_deleted=data.get("num_deleted", 0),
            num_subjects=data.get("num_subjects", 0),
            deleted=data.get("deleted", None),
            subjects=data.get("subjects", None),
        )


@dataclass
class Republish:
    """
    RePublish is for republishing messages once committed to a stream. The
    original subject is remapped from the subject pattern to the destination
    pattern.
    """

    destination: str
    """The subject pattern to republish the subject to."""

    source: Optional[str] = None
    """The subject pattern to match incoming messages against."""

    headers_only: Optional[bool] = None
    """A flag to indicate that only the headers should be republished."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Republish:
        return cls(
            destination=data["dest"],
            source=data.get("src"),
            headers_only=data.get("headers_only"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            key: value
            for key, value in {
                "dest": self.destination,
                "src": self.source,
                "headers_only": self.headers_only,
            }.items()
            if value is not None
        }


@dataclass
class Placement:
    """
    Placement is used to guide placement of streams in clustered JetStream.
    """

    cluster: str
    """The name of the cluster to which the stream should be assigned."""

    tags: List[str] = field(default_factory=list)
    """Tags are used to match streams to servers in the cluster. A stream will be assigned to a server with a matching tag."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Placement:
        return cls(
            cluster=data["cluster"],
            tags=data.get("tags", []),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cluster": self.cluster,
            "tags": self.tags,
        }


@dataclass
class ExternalStream:
    """
    ExternalStream allows you to qualify access to a stream source in another
    account.
    """

    api_prefix: str
    """The subject prefix that imports the other account/domain $JS.API.CONSUMER.> subjects."""

    deliver_prefix: str
    """The delivery subject to use for the push consumer."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ExternalStream:
        return cls(
            api_prefix=data["api"],
            deliver_prefix=data["deliver"],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "api": self.api_prefix,
            "deliver": self.deliver_prefix,
        }


@dataclass
class StreamSource:
    """
    StreamSource dictates how streams can source from other streams.
    """

    name: str
    """The name of the stream to source from."""

    opt_start_seq: Optional[int] = None
    """The sequence number to start sourcing from."""

    opt_start_time: Optional[datetime] = None
    """The timestamp of messages to start sourcing from."""

    filter_subject: Optional[str] = None
    """The subject filter used to only replicate messages with matching subjects."""

    subject_transforms: Optional[List[SubjectTransformConfig]] = None
    """
    A list of subject transforms to apply to matching messages.

    Subject transforms on sources and mirrors are also used as subject filters with optional transformations.
    """

    external: Optional[ExternalStream] = None
    """A configuration referencing a stream source in another account or JetStream domain."""

    domain: Optional[str] = None
    """Used to configure a stream source in another JetStream domain. This setting will set the External field with the appropriate APIPrefix."""

    def __post__init__(self):
        if self.external and self.domain:
            raise ValueError("cannot set both external and domain")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StreamSource:
        kwargs = data.copy()

        return cls(
            name=data["name"],
            opt_start_seq=data.get("opt_start_seq"),
            opt_start_time=data.get("opt_start_time"),
            filter_subject=data.get("filter_subject"),
            subject_transforms=[
                SubjectTransformConfig.from_dict(subject_transform)
                for subject_transform in data.get("subject_transforms", [])
            ],
            external=ExternalStream.from_dict(data["external"])
            if data.get("external")
            else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "opt_start_seq": self.opt_start_seq,
            "opt_start_time": self.opt_start_time,
            "filter_subject": self.filter_subject,
            "subject_transforms": [
                subject_transform.to_dict()
                for subject_transform in self.subject_transforms
            ]
            if self.subject_transforms
            else None,
            "external": self.external.to_dict() if self.external else None,
        }


@dataclass
class StreamConsumerLimits:
    """
    StreamConsumerLimits are the limits for a consumer on a stream. These can
    be overridden on a per consumer basis.
    """

    inactive_threshold: Optional[int] = None
    """A duration which instructs the server to clean up the consumer if it has been inactive for the specified duration."""

    max_ack_pending: Optional[int] = None
    """A maximum number of outstanding unacknowledged messages for a consumer."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StreamConsumerLimits:
        return cls(
            max_ack_pending=data.get("max_ack_pending"),
            inactive_threshold=data.get("inactive_threshold"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_ack_pending": self.max_ack_pending,
            "inactive_threshold": self.inactive_threshold,
        }


@dataclass
class StreamConfig:
    """
    StreamConfig is the configuration of a JetStream stream.
    """

    name: str
    """Name is the name of the stream. It is required and must be unique across the JetStream account. Names cannot contain whitespace, ., >, path separators (forward or backwards slash), and non-printable characters."""

    description: Optional[str] = None
    """Description is an optional description of the stream."""

    subjects: List[str] = field(default_factory=list)
    """Subjects is a list of subjects that the stream is listening on. Wildcards are supported. Subjects cannot be set if the stream is created as a mirror."""

    retention: Optional[RetentionPolicy] = None
    """Retention defines the message retention policy for the stream. Defaults to RetentionPolicy.LIMITS."""

    max_consumers: Optional[int] = None
    """MaxConsumers specifies the maximum number of consumers allowed for the stream."""

    max_msgs: Optional[int] = None
    """MaxMsgs is the maximum number of messages the stream will store. After reaching the limit, stream adheres to the discard policy. If not set, server default is -1 (unlimited)."""

    max_bytes: Optional[int] = None
    """MaxBytes is the maximum total size of messages the stream will store. After reaching the limit, stream adheres to the discard policy. If not set, server default is -1 (unlimited)."""

    discard: Optional[DiscardPolicy] = None
    """Discard defines the policy for handling messages when the stream reaches its limits in terms of number of messages or total bytes. Defaults to DiscardPolicy.OLD if not set"""

    discard_new_per_subject: Optional[bool] = None
    """DiscardNewPerSubject is a flag to enable discarding new messages per subject when limits are reached. Requires DiscardPolicy to be DiscardNew and the MaxMsgsPerSubject to be set."""

    max_age: Optional[int] = None
    """MaxAge is the maximum age of messages that the stream will retain."""

    max_msgs_per_subject: Optional[int] = None
    """MaxMsgsPerSubject is the maximum number of messages per subject that the stream will retain."""

    max_msg_size: Optional[int] = None
    """MaxMsgSize is the maximum size of any single message in the stream."""

    storage: StorageType = StorageType.FILE
    """Storage specifies the type of storage backend used for the stream (file or memory). Defaults to StorageType.FILE """

    replicas: int = 1
    """Replicas is the number of stream replicas in clustered JetStream. Defaults to 1, maximum is 5."""

    no_ack: Optional[bool] = None
    """NoAck is a flag to disable acknowledging messages received by this stream. If set to true, publish methods from the JetStream client will not work as expected, since they rely on acknowledgements. Core NATS publish methods should be used instead. Note that this will make message delivery less reliable."""

    duplicates: Optional[int] = None
    """Duplicates is the window within which to track duplicate messages. If not set, server default is 2 minutes."""

    placement: Optional[Placement] = None
    """Placement is used to declare where the stream should be placed via tags and/or an explicit cluster name."""

    mirror: Optional[StreamSource] = None
    """Mirror defines the configuration for mirroring another stream."""

    sources: Optional[List[StreamSource]] = None
    """Sources is a list of other streams this stream sources messages from."""

    sealed: Optional[bool] = None
    """Sealed streams do not allow messages to be published or deleted via limits or API, sealed streams cannot be unsealed via configuration update. Can only be set on already created streams via the Update API."""

    deny_delete: Optional[bool] = None
    """
    Restricts the ability to delete messages from a stream via the API.

    Server defaults to false when not set.
    """

    deny_purge: Optional[bool] = None
    """Restricts the ability to purge messages from a stream via the API.

    Server defaults to false from server when not set.
    """

    allow_rollup: Optional[bool] = None
    """Allows the use of the `Nats-Rollup` header to replace all contents of a stream, or subject in a stream, with a single new message.
    """

    compression: Optional[StoreCompression] = None
    """
    Specifies the message storage compression algorithm.

    Server defaults to `StoreCompression.NONE` when not set.
    """

    first_sequence: Optional[int] = None
    """The initial sequence number of the first message in the stream."""

    subject_transform: Optional[SubjectTransformConfig] = None
    """Allows applying a transformation to matching messages' subjects."""

    republish: Optional[Republish] = None
    """Allows immediate republishing of a message to the configured subject after it's stored."""

    allow_direct: bool = False
    """
    Enables direct access to individual messages using direct get.

    Server defaults to false.
    """

    mirror_direct: bool = False
    """
    Enables direct access to individual messages from the origin stream.

    Defaults to false.
    """

    consumer_limits: Optional[StreamConsumerLimits] = None
    """Defines limits of certain values that consumers can set, defaults for those who don't set these settings."""

    metadata: Optional[Dict[str, str]] = None
    """Provides a set of application-defined key-value pairs for associating metadata on the stream.

    Note: This feature requires nats-server v2.10.0 or later.
    """

    def __post_init__(self):
        _validate_stream_name(self.name)

        if self.max_msgs_per_subject and not self.discard:
            raise ValueError("max_msgs_per_subject requires discard policy to be set")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StreamConfig:
        return cls(
            name=data["name"],
            description=data.get("description"),
            subjects=data.get("subjects", []),
            retention=RetentionPolicy(data["retention"])
            if data.get("retention")
            else None,
            max_consumers=data.get("max_consumers"),
            max_msgs=data.get("max_msgs"),
            max_bytes=data.get("max_bytes"),
            discard=DiscardPolicy(data["discard"]) if data.get("discard") else None,
            discard_new_per_subject=data.get("discard_new_per_subject"),
            max_age=data.get("max_age"),
            max_msgs_per_subject=data.get("max_msgs_per_subject"),
            max_msg_size=data.get("max_msg_size"),
            storage=StorageType(data["storage"]),
            replicas=data.get("num_replicas", 1),
            no_ack=data.get("no_ack"),
            duplicates=data.get("duplicates"),
            placement=Placement.from_dict(data["placement"])
            if data.get("placement")
            else None,
            mirror=StreamSource.from_dict(data["mirror"])
            if data.get("mirror")
            else None,
            sources=[
                StreamSource.from_dict(source) for source in data.get("sources", [])
            ],
            sealed=data.get("sealed"),
            deny_delete=data.get("deny_delete"),
            deny_purge=data.get("deny_purge"),
            allow_rollup=data.get("allow_rollup"),
            compression=StoreCompression(data["compression"])
            if data.get("compression")
            else None,
            first_sequence=data.get("first_sequence"),
            subject_transform=SubjectTransformConfig.from_dict(
                data["subject_transform"]
            )
            if data.get("subject_transform")
            else None,
            republish=Republish.from_dict(data["republish"])
            if data.get("republish")
            else None,
            allow_direct=data.get("allow_direct", False),
            mirror_direct=data.get("mirror_direct", False),
            consumer_limits=StreamConsumerLimits.from_dict(data["consumer_limits"])
            if data.get("consumer_limits")
            else None,
            metadata=data.get("metadata"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            k: v
            for k, v in {
                "name": self.name,
                "description": self.description,
                "subjects": self.subjects,
                "retention": self.retention.value if self.retention else None,
                "max_consumers": self.max_consumers,
                "max_msgs": self.max_msgs,
                "max_bytes": self.max_bytes,
                "discard": self.discard.value if self.discard else None,
                "discard_new_per_subject": self.discard_new_per_subject,
                "max_age": self.max_age,
                "max_msgs_per_subject": self.max_msgs_per_subject,
                "max_msg_size": self.max_msg_size,
                "storage": self.storage.value,
                "num_replicas": self.replicas,
                "no_ack": self.no_ack,
                "duplicate_window": self.duplicates,
                "placement": self.placement.to_dict() if self.placement else None,
                "mirror": self.mirror.to_dict() if self.mirror else None,
                "sources": [source.to_dict() for source in self.sources]
                if self.sources
                else None,
                "sealed": self.sealed,
                "deny_delete": self.deny_delete,
                "deny_purge": self.deny_purge,
                "allow_rollup": self.allow_rollup,
                "compression": self.compression.value if self.compression else None,
                "first_seq": self.first_sequence,
                "subject_transform": self.subject_transform.to_dict()
                if self.subject_transform
                else None,
                "republish": self.republish.to_dict() if self.republish else None,
                "allow_direct": self.allow_direct,
                "mirror_direct": self.mirror_direct,
                "consumer_limits": self.consumer_limits.to_dict()
                if self.consumer_limits
                else None,
                "metadata": self.metadata,
            }.items()
            if v is not None
        }


@dataclass
class StreamInfo:
    """
    Provides configuration and current state for a stream.
    """

    config: StreamConfig
    """Contains the configuration settings of the stream, set when creating or updating the stream."""

    timestamp: datetime
    """Indicates when the info was gathered by the server."""

    created: datetime
    """The timestamp when the stream was created."""

    state: StreamState
    """Provides the state of the stream at the time of request, including metrics like the number of messages in the stream, total bytes, etc."""

    cluster: Optional[ClusterInfo] = None
    """Contains information about the cluster to which this stream belongs (if applicable)."""

    mirror: Optional[StreamSourceInfo] = None
    """Contains information about another stream this one is mirroring. Mirroring is used to create replicas of another stream's data. This field is omitted if the stream is not mirroring another stream."""

    sources: List[StreamSourceInfo] = field(default_factory=list)
    """A list of source streams from which this stream collects data."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StreamInfo:
        return cls(
            config=StreamConfig.from_dict(data["config"]),
            timestamp=datetime.fromisoformat(data["ts"]),
            created=datetime.fromisoformat(data["created"]),
            state=StreamState.from_dict(data["state"]),
            cluster=ClusterInfo.from_dict(data["cluster"])
            if "cluster" in data
            else None,
            mirror=StreamSourceInfo.from_dict(data["mirror"])
            if "mirror" in data
            else None,
            sources=[StreamSourceInfo.from_dict(source) for source in data["sources"]]
            if "sources" in data
            else [],
        )


class StreamNameAlreadyInUse(Exception):
    """
    Raised when trying to create a stream with a name that is already in use.
    """

    pass


class StreamNotFoundError(Exception):
    """
    Raised when trying to access a stream that does not exist.
    """

    pass


class StreamSourceNotSupportedError(Exception):
    """
    Raised when a source stream is not supported by the server.
    """

    pass


class StreamSubjectTransformNotSupportedError(Exception):
    """
    Raised when a subject transform is not supported by the server.
    """

    pass


class StreamSourceMultipleFilterSubjectsNotSupported(Exception):
    """
    Raised when multiple filter subjects are not supported by the server.
    """

    pass


class InvalidStreamNameError(ValueError):
    """
    Raised when an invalid stream name is provided.
    """

    pass


class StreamNameRequiredError(ValueError):
    """
    Raised when a stream name is required but not provided (e.g empty).
    """

    pass


class StreamNameAlreadyInUseError(Exception):
    """
    Raised when a stream name is already in use.
    """

    pass


class Stream:
    def __init__(self, client: Client, name: str, info: StreamInfo) -> None:
        self._client = client
        self._name = name
        self._cached_info = info

    @property
    def cached_info(self) -> StreamInfo:
        """
        Returns the cached `StreamInfo` for the stream.
        """
        return self._cached_info

    async def info(
        self,
        subject_filter: Optional[str] = None,
        deleted_details: Optional[bool] = None,
        timeout: Optional[float] = None
    ) -> StreamInfo:
        """Returns `StreamInfo` from the server."""
        # TODO(caspervonb): handle pagination
        stream_info_subject = f"STREAM.INFO.{self._name}"
        stream_info_request = {
            "subject_filter": subject_filter,
            "deleted_details": deleted_details,
        }
        try:
            info_response = await self._client.request_json(
                stream_info_subject, stream_info_request, timeout=timeout
            )
        except JetStreamError as jetstream_error:
            if jetstream_error.code == STREAM_NOT_FOUND:
                raise StreamNotFoundError() from jetstream_error

            raise jetstream_error

        info = StreamInfo.from_dict(info_response)
        self._cached_info = info

        return info


    # TODO(caspervonb): Go does not return anything for this operation, should we?
    async def purge(
        self,
        sequence: Optional[int] = None,
        keep: Optional[int] = None,
        subject: Optional[str] = None,
        timeout: Optional[float] = None
    ) -> None:
        """
        Removes messages from a stream.
        This is a destructive operation.
        """

        # TODO(caspervonb): enforce types with overloads
        if keep is not None and sequence is not None:
            raise ValueError(
                "both 'keep' and 'sequence' cannot be provided in purge request"
            )

        stream_purge_subject = f"STREAM.PURGE.{self._name}"
        stream_purge_request = {
            "sequence": sequence,
            "keep": keep,
            "subject": subject,
        }

        try:
            stream_purge_response = await self._client.request_json(
                stream_purge_subject, stream_purge_request, timeout=timeout
            )
        except JetStreamError as jetstream_error:
            raise jetstream_error


    async def create_consumer(
        self, config: ConsumerConfig, timeout: Optional[float] = None
    ) -> Consumer:
        return await _create_consumer(
            self._client, stream=self._name, config=config, timeout=timeout
        )

    async def update_consumer(
        self, config: ConsumerConfig, timeout: Optional[float] = None
    ) -> Consumer:
        return await _update_consumer(
            self._client, stream=self._name, config=config, timeout=timeout
        )

    async def create_or_update_consumer(
        self, config: ConsumerConfig, timeout: Optional[float] = None
    ) -> Consumer:
        return await _create_or_update_consumer(
            self._client, stream=self._name, config=config, timeout=timeout
        )

    async def consumer(self, name: str, timeout: Optional[float] = None) -> Consumer:
        return await _get_consumer(
            self._client, stream=self._name, name=name, timeout=timeout
        )

    async def delete_consumer(self, name: str, timeout: Optional[float] = None) -> None:
        return await _delete_consumer(
            self._client, stream=self._name, consumer=name, timeout=timeout
        )


class StreamNameLister(AsyncIterable):
    pass


class StreamInfoLister(AsyncIterable):
    pass


class StreamManager(Protocol):
    """
    Provides methods for managing streams.
    """

    async def create_stream(
        self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream:
        """
        Creates a new stream with given config.
        """
        ...

    async def update_stream(
        self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream:
        """
        Updates an existing stream with the given config.
        """
        ...

    async def create_or_update_stream(
        self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream: ...

    async def stream(self, name: str, timeout: Optional[float] = None) -> Stream:
        """Fetches `StreamInfo` and returns a `Stream` instance for a given stream name."""
        ...

    async def stream_name_by_subject(
        self, subject: str, timeout: Optional[float] = None
    ) -> str:
        """Returns a stream name listening on a given subject."""
        ...

    async def delete_stream(self, name: str, timeout: Optional[float] = None) -> None:
        """Removes a stream with given name."""
        ...

    def list_streams(self, timeout: Optional[float] = None) -> StreamInfoLister:
        """Returns a `StreamLister` for iterating over stream infos."""
        ...

    def stream_names(self, timeout: Optional[float] = None) -> StreamNameLister:
        """Returns a `StreamNameLister` for iterating over stream names."""
        ...


def _validate_stream_name(stream_name: str) -> None:
    if not stream_name:
        raise StreamNameRequiredError()

    invalid_chars = ">*. /\\"
    if any(char in stream_name for char in invalid_chars):
        raise InvalidStreamNameError(stream_name)
