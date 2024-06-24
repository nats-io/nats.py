from __future__ import annotations
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional
import datetime

@dataclass
class StreamInfo:
    """
    StreamInfo shows config and current state for this stream.
    """

    config: StreamConfig = field(metadata={'json': 'config'})
    """Contains the configuration settings of the stream, set when creating or updating the stream."""

    created: datetime.datetime = field(metadata={'json': 'created'})
    """The timestamp when the stream was created."""

    state: StreamState = field(metadata={'json': 'state'})
    """Provides the state of the stream at the time of request, including metrics like the number of messages in the stream, total bytes, etc."""

    cluster: Optional[ClusterInfo] = field(default=None, metadata={'json': 'cluster'})
    """Contains information about the cluster to which this stream belongs (if applicable)."""

    mirror: Optional[StreamSourceInfo] = field(default=None, metadata={'json': 'mirror'})
    """Contains information about another stream this one is mirroring. Mirroring is used to create replicas of another stream's data. This field is omitted if the stream is not mirroring another stream."""

    sources: List[StreamSourceInfo] = field(default_factory=list, metadata={'json': 'sources'})
    """A list of source streams from which this stream collects data."""

    timestamp: datetime.datetime = field(metadata={'json': 'ts'})
    """Indicates when the info was gathered by the server."""

@dataclass
class StreamConfig:
    """
    StreamConfig is the configuration of a JetStream stream.
    """

    name: str = field(metadata={'json': 'name'})
    """Name is the name of the stream. It is required and must be unique across the JetStream account. Names cannot contain whitespace, ., *, >, path separators (forward or backwards slash), and non-printable characters."""

    description: Optional[str] = field(default=None, metadata={'json': 'description'})
    """Description is an optional description of the stream."""

    subjects: List[str] = field(default_factory=list, metadata={'json': 'subjects'})
    """Subjects is a list of subjects that the stream is listening on. Wildcards are supported. Subjects cannot be set if the stream is created as a mirror."""

    retention: RetentionPolicy = field(default=RetentionPolicy.LIMIT, metadata={'json': 'retention'})
    """Retention defines the message retention policy for the stream. Defaults to LimitsPolicy."""

    max_consumers: int = field(metadata={'json': 'max_consumers'})
    """MaxConsumers specifies the maximum number of consumers allowed for the stream."""

    max_msgs: int = field(metadata={'json': 'max_msgs'})
    """MaxMsgs is the maximum number of messages the stream will store. After reaching the limit, stream adheres to the discard policy. If not set, server default is -1 (unlimited)."""

    max_bytes: int = field(metadata={'json': 'max_bytes'})
    """MaxBytes is the maximum total size of messages the stream will store. After reaching the limit, stream adheres to the discard policy. If not set, server default is -1 (unlimited)."""

    discard: DiscardPolicy = field(metadata={'json': 'discard'})
    """Discard defines the policy for handling messages when the stream reaches its limits in terms of number of messages or total bytes."""

    discard_new_per_subject: Optional[bool] = field(default=None, metadata={'json': 'discard_new_per_subject'})
    """DiscardNewPerSubject is a flag to enable discarding new messages per subject when limits are reached. Requires DiscardPolicy to be DiscardNew and the MaxMsgsPerSubject to be set."""

    max_age: datetime.timedelta = field(metadata={'json': 'max_age'})
    """MaxAge is the maximum age of messages that the stream will retain."""

    max_msgs_per_subject: int = field(metadata={'json': 'max_msgs_per_subject'})
    """MaxMsgsPerSubject is the maximum number of messages per subject that the stream will retain."""

    max_msg_size: Optional[int] = field(default=None, metadata={'json': 'max_msg_size'})
    """MaxMsgSize is the maximum size of any single message in the stream."""

    storage: StorageType = field(metadata={'json': 'storage'})
    """Storage specifies the type of storage backend used for the stream (file or memory)."""

    replicas: int = field(default=1, metadata={'json': 'num_replicas'})
    """Replicas is the number of stream replicas in clustered JetStream. Defaults to 1, maximum is 5."""

    no_ack: Optional[bool] = field(default=None, metadata={'json': 'no_ack'})
    """NoAck is a flag to disable acknowledging messages received by this stream. If set to true, publish methods from the JetStream client will not work as expected, since they rely on acknowledgements. Core NATS publish methods should be used instead. Note that this will make message delivery less reliable."""

    duplicates: Optional[datetime.timedelta] = field(default=None, metadata={'json': 'duplicate_window'})
    """Duplicates is the window within which to track duplicate messages. If not set, server default is 2 minutes."""

    placement: Optional[Placement] = field(default=None, metadata={'json': 'placement'})
    """Placement is used to declare where the stream should be placed via tags and/or an explicit cluster name."""

    mirror: Optional[StreamSource] = field(default=None, metadata={'json': 'mirror'})
    """Mirror defines the configuration for mirroring another stream."""

    sources: List[StreamSource] = field(default_factory=list, metadata={'json': 'sources'})
    """Sources is a list of other streams this stream sources messages from."""

    sealed: Optional[bool] = field(default=None, metadata={'json': 'sealed'})
    """Sealed streams do not allow messages to be published or deleted via limits or API, sealed streams cannot be unsealed via configuration update. Can only be set on already created streams via the Update API."""

    deny_delete: Optional[bool] = field(default=None, metadata={'json': 'deny_delete'})
    """DenyDelete restricts the ability to delete messages from a stream via the API. Defaults to false."""

    deny_purge: Optional[bool] = field(default=None, metadata={'json': 'deny_purge'})
    """DenyPurge restricts the ability to purge messages from a stream via the API. Defaults to false."""

    allow_rollup: Optional[bool] = field(default=None, metadata={'json': 'allow_rollup_hdrs'})
    """AllowRollup allows the use of the Nats-Rollup header to replace all contents of a stream, or subject in a stream, with a single new message."""

    compression: StoreCompression = field(default=StoreCompression.NONE, metadata={'json': 'compression'})
    """Compression specifies the message storage compression algorithm. Defaults to NoCompression."""

    first_sequence: Optional[int] = field(default=None, metadata={'json': 'first_seq'})
    """FirstSeq is the initial sequence number of the first message in the stream."""

    subject_transform: Optional[SubjectTransformConfig] = field(default=None, metadata={'json': 'subject_transform'})
    """SubjectTransform allows applying a transformation to matching messages' subjects."""

    republish: Optional[Republish] = field(default=None, metadata={'json': 'republish'})
    """RePublish allows immediate republishing of a message to the configured subject after it's stored."""

    allow_direct: bool = field(default=False, metadata={'json': 'allow_direct'})
    """AllowDirect enables direct access to individual messages using direct get API. Defaults to false."""

    mirror_direct: bool = field(default=False, metadata={'json': 'mirror_direct'})
    """MirrorDirect enables direct access to individual messages from the origin stream using direct get API. Defaults to false."""

    consumer_limits: Optional[StreamConsumerLimits] = field(default=None, metadata={'json': 'consumer_limits'})
    """ConsumerLimits defines limits of certain values that consumers can set, defaults for those who don't set these settings."""

    metadata: Dict[str, str] = field(default_factory=dict, metadata={'json': 'metadata'})
    """Metadata is a set of application-defined key-value pairs for associating metadata on the stream. This feature requires nats-server v2.10.0 or later."""

    template: Optional[str] = field(default=None, metadata={'json': 'template_owner'})
    """Template identifies the template that manages the Stream. DEPRECATED: This feature is no longer supported."""

@dataclass
class StreamSourceInfo:
    """
    StreamSourceInfo shows information about an upstream stream source/mirror.
    """

    name: str = field(metadata={'json': 'name'})
    """Name is the name of the stream that is being replicated."""

    lag: int = field(metadata={'json': 'lag'})
    """Lag informs how many messages behind the source/mirror operation is. This will only show correctly if there is active communication with stream/mirror."""

    active: datetime.timedelta = field(metadata={'json': 'active'})
    """Active informs when last the mirror or sourced stream had activity. Value will be -1 when there has been no activity."""

    filter_subject: Optional[str] = field(default=None, metadata={'json': 'filter_subject'})
    """FilterSubject is the subject filter defined for this source/mirror."""

    subject_transforms: List[SubjectTransformConfig] = field(default_factory=list, metadata={'json': 'subject_transforms'})
    """SubjectTransforms is a list of subject transforms defined for this source/mirror."""

@dataclass
class StreamState:
    """
    StreamState is the state of a JetStream stream at the time of request.
    """

    msgs: int = field(metadata={'json': 'messages'})
    """Msgs is the number of messages stored in the stream."""

    bytes: int = field(metadata={'json': 'bytes'})
    """Bytes is the number of bytes stored in the stream."""

    first_sequence: int = field(metadata={'json': 'first_seq'})
    """FirstSeq is the sequence number of the first message in the stream."""

    first_time: datetime.datetime = field(metadata={'json': 'first_ts'})
    """FirstTime is the timestamp of the first message in the stream."""

    last_sequence: int = field(metadata={'json': 'last_seq'})
    """LastSeq is the sequence number of the last message in the stream."""

    last_time: datetime.datetime = field(metadata={'json': 'last_ts'})
    """LastTime is the timestamp of the last message in the stream."""

    consumers: int = field(metadata={'json': 'consumer_count'})
    """Consumers is the number of consumers on the stream."""

    deleted: List[int] = field(default_factory=list, metadata={'json': 'deleted'})
    """Deleted is a list of sequence numbers that have been removed from the stream. This field will only be returned if the stream has been fetched with the DeletedDetails option."""

    num_deleted: int = field(metadata={'json': 'num_deleted'})
    """NumDeleted is the number of messages that have been removed from the stream. Only deleted messages causing a gap in stream sequence numbers are counted. Messages deleted at the beginning or end of the stream are not counted."""

    num_subjects: int = field(metadata={'json': 'num_subjects'})
    """NumSubjects is the number of unique subjects the stream has received messages on."""

    subjects: Dict[str, int] = field(default_factory=dict, metadata={'json': 'subjects'})
    """Subjects is a map of subjects the stream has received messages on with message count per subject. This field will only be returned if the stream has been fetched with the SubjectFilter option."""

@dataclass
class ClusterInfo:
    """
    ClusterInfo shows information about the underlying set of servers that
    make up the stream or consumer.
    """

    name: Optional[str] = field(default=None, metadata={'json': 'name'})
    """Name is the name of the cluster."""

    leader: Optional[str] = field(default=None, metadata={'json': 'leader'})
    """Leader is the server name of the RAFT leader."""

    replicas: List[PeerInfo] = field(default_factory=list, metadata={'json': 'replicas'})
    """Replicas is the list of members of the RAFT cluster."""


@dataclass
class PeerInfo:
    """
    PeerInfo shows information about the peers in the cluster that are
    supporting the stream or consumer.
    """

    name: str = field(metadata={'json': 'name'})
    """The server name of the peer."""

    current: bool = field(metadata={'json': 'current'})
    """Indicates if the peer is up to date and synchronized with the leader."""

    offline: Optional[bool] = field(default=None, metadata={'json': 'offline'})
    """Indicates if the peer is considered offline by the group."""

    active: datetime.timedelta = field(metadata={'json': 'active'})
    """The duration since this peer was last seen."""

    lag: Optional[int] = field(default=None, metadata={'json': 'lag'})
    """The number of uncommitted operations this peer is behind the leader."""


@dataclass
class SubjectTransformConfig:
    """
    SubjectTransformConfig is for applying a subject transform (to matching
    messages) before doing anything else when a new message is received.
    """

    source: str = field(metadata={'json': 'src'})
    """The subject pattern to match incoming messages against."""

    destination: str = field(metadata={'json': 'dest'})
    """The subject pattern to remap the subject to."""


@dataclass
class Republish:
    """
    RePublish is for republishing messages once committed to a stream. The
    original subject is remapped from the subject pattern to the destination
    pattern.
    """

    source: Optional[str] = field(default=None, metadata={'json': 'src'})
    """The subject pattern to match incoming messages against."""

    destination: str = field(metadata={'json': 'dest'})
    """The subject pattern to republish the subject to."""

    headers_only: Optional[bool] = field(default=None, metadata={'json': 'headers_only'})
    """A flag to indicate that only the headers should be republished."""


@dataclass
class Placement:
    """
    Placement is used to guide placement of streams in clustered JetStream.
    """

    cluster: str = field(metadata={'json': 'cluster'})
    """The name of the cluster to which the stream should be assigned."""

    tags: List[str] = field(default_factory=list, metadata={'json': 'tags'})
    """Tags are used to match streams to servers in the cluster. A stream will be assigned to a server with a matching tag."""


@dataclass
class StreamSource:
    """
    StreamSource dictates how streams can source from other streams.
    """

    name: str = field(metadata={'json': 'name'})
    """The name of the stream to source from."""

    opt_start_seq: Optional[int] = field(default=None, metadata={'json': 'opt_start_seq'})
    """The sequence number to start sourcing from."""

    opt_start_time: Optional[datetime.datetime] = field(default=None, metadata={'json': 'opt_start_time'})
    """The timestamp of messages to start sourcing from."""

    filter_subject: Optional[str] = field(default=None, metadata={'json': 'filter_subject'})
    """The subject filter used to only replicate messages with matching subjects."""

    subject_transforms: List[SubjectTransformConfig] = field(default_factory=list, metadata={'json': 'subject_transforms'})
    """
    A list of subject transforms to apply to matching messages.

    Subject transforms on sources and mirrors are also used as subject filters with optional transformations.
    """

    external: Optional[ExternalStream] = field(default=None, metadata={'json': 'external'})
    """A configuration referencing a stream source in another account or JetStream domain."""

    domain: Optional[str] = field(default=None, metadata={'json': '-'})
    """Used to configure a stream source in another JetStream domain. This setting will set the External field with the appropriate APIPrefix."""


@dataclass
class ExternalStream:
    """
    ExternalStream allows you to qualify access to a stream source in another
    account.
    """

    api_prefix: str = field(metadata={'json': 'api'})
    """The subject prefix that imports the other account/domain $JS.API.CONSUMER.> subjects."""

    deliver_prefix: str = field(metadata={'json': 'deliver'})
    """The delivery subject to use for the push consumer."""


@dataclass
class StreamConsumerLimits:
    """
    StreamConsumerLimits are the limits for a consumer on a stream. These can
    be overridden on a per consumer basis.
    """

    inactive_threshold: Optional[datetime.timedelta] = field(default=None, metadata={'json': 'inactive_threshold'})
    """A duration which instructs the server to clean up the consumer if it has been inactive for the specified duration."""

    max_ack_pending: Optional[int] = field(default=None, metadata={'json': 'max_ack_pending'})
    """A maximum number of outstanding unacknowledged messages for a consumer."""


class RetentionPolicy(Enum):
    """
    RetentionPolicy determines how messages in a stream are retained.
    """

    LIMITS = "limits"
    """LimitsPolicy means that messages are retained until any given limit is reached. This could be one of MaxMsgs, MaxBytes, or MaxAge."""

    INTEREST = "interest"
    """InterestPolicy specifies that when all known observables have acknowledged a message, it can be removed."""

    WORKQUEUE = "workqueue"
    """WorkQueuePolicy specifies that when the first worker or subscriber acknowledges the message, it can be removed."""


class DiscardPolicy(Enum):
    """
    DiscardPolicy determines how to proceed when limits of messages or bytes
    are reached.
    """

    OLD = "old"
    """DiscardOld will remove older messages to return to the limits. This is the default."""

    NEW = "new"
    """DiscardNew will fail to store new messages once the limits are reached."""


class StorageType(Enum):
    """
    StorageType determines how messages are stored for retention.
    """

    FILE = "file"
    """
    Specifies on disk storage.
    """

    MEMORY = "memory"
    """
    Specifies in-memory storage.
    """


class StoreCompression(Enum):
    """
    StoreCompression determines how messages are compressed.
    """

    NONE = "none"
    """
    Disables compression on the stream.
    """

    S2 = "s2"
    """
    Enables S2 compression on the stream.
    """


class Stream:
    """
    Stream contains operations on an existing stream. It allows fetching and removing
    messages from a stream, as well as purging a stream.
    """

    def __init__(self, info: StreamInfo):
        self._info = info

    async def info(self, opts: Optional[List[Any]] = None, *, timeout: Optional[int] = None) -> StreamInfo:
        """Info returns StreamInfo from the server."""
        pass

    def cached_info(self) -> StreamInfo:
        """CachedInfo returns StreamInfo currently cached on this stream."""
        return self._info

    async def purge(self, opts: Optional[List[Any]] = None, *, timeout: Optional[int] = None) -> None:
        """
        Removes messages from a stream.
        This is a destructive operation.
        """
        pass

    async def get_msg(self, seq: int, opts: Optional[List[Any]] = None, *, timeout: Optional[int] = None) -> RawStreamMsg:
        """
        Retrieves a raw stream message stored in JetStream by sequence number.
        """
        pass

    async def get_last_msg_for_subject(self, subject: str, *, timeout: Optional[int] = None) -> RawStreamMsg:
        """
        Retrieves the last raw stream message stored in JetStream on a given subject.
        """
        pass

    async def delete_msg(self, seq: int, *, timeout: Optional[int] = None) -> None:
        """
        Deletes a message from a stream.
        """
        pass

    async def secure_delete_msg(self, seq: int, *, timeout: Optional[int] = None) -> None:
        """
        Deletes a message from a stream.
        """
        pass


class StreamManager:
    """
    Provides methods for managing streams.
    """

    async def create_stream(self, config: StreamConfig, *, timeout: Optional[int] = None) -> Stream:
        """
        Creates a new stream with given config.
        """
        pass

    async def update_stream(self, config: StreamConfig, *, timeout: Optional[int] = None) -> Stream:
        """
        Updates an existing stream with the given config.
        """
        pass

    async def create_or_update_stream(self, cfg: StreamConfig, *, timeout: Optional[int] = None) -> Stream:
        """CreateOrUpdateStream creates a stream with given config or updates it if it already exists."""
        pass

    async def stream(self, stream: str, *, timeout: Optional[int] = None) -> Stream:
        """Stream fetches StreamInfo and returns a Stream interface for a given stream name."""
        pass

    async def stream_name_by_subject(self, subject: str, *, timeout: Optional[int] = None) -> str:
        """StreamNameBySubject returns a stream name listening on a given subject."""
        pass

    async def delete_stream(self, stream: str, *, timeout: Optional[int] = None) -> None:
        """DeleteStream removes a stream with given name."""
        pass

    def list_streams(self, *, timeout: Optional[int] = None) -> StreamInfoLister:
        """ListStreams returns a StreamInfoLister for iterating over stream infos."""
        pass

    def stream_names(self, *, timeout: Optional[int] = None) -> StreamNameLister:
        """StreamNames returns a StreamNameLister for iterating over stream names."""
        pass
