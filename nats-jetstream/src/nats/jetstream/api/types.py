from typing import Any, Literal, NotRequired, TypedDict, Union


class SubjectTransform(TypedDict):
    """Subject transform to apply to matching messages going into the stream"""

    dest: str
    """The subject transform destination"""

    src: str
    """The subject transform source"""


class ExternalStreamSource(TypedDict):
    """Configuration referencing a stream source in another account or JetStream domain"""

    api: str
    """The subject prefix that imports the other account/domain $JS.API.CONSUMER.> subjects"""

    deliver: NotRequired[str]
    """The delivery subject to use for the push consumer"""


class StreamSource(TypedDict):
    """Defines a source where streams should be replicated from"""

    external: NotRequired[ExternalStreamSource]

    filter_subject: NotRequired[str]
    """Replicate only a subset of messages based on filter"""

    name: str
    """Stream name"""

    opt_start_seq: NotRequired[int]
    """Sequence to start replicating from"""

    opt_start_time: NotRequired[int]
    """Time stamp to start replicating from"""

    subject_transforms: NotRequired[Any]
    """The subject filtering sources and associated destination transforms"""


class PriorityGroupState(TypedDict):
    """Status of a specific consumer priority group"""

    group: str
    """The group this status is for"""

    pinned_client_id: NotRequired[str]
    """The generated ID of the pinned client"""

    pinned_ts: NotRequired[int]
    """The timestamp when the client was pinned"""


class Error(TypedDict):
    code: int
    """HTTP like error code in the 300 to 500 range"""

    description: NotRequired[str]
    """A human friendly description of the error"""

    err_code: NotRequired[int]
    """The NATS error code unique to each kind of error"""


class StreamSourceInfo(TypedDict):
    """Information about an upstream stream source in a mirror"""

    active: int
    """When last the mirror had activity, in nanoseconds. Value will be -1 when there has been no activity."""

    error: NotRequired[Error]

    external: NotRequired[ExternalStreamSource]

    filter_subject: NotRequired[str]
    """The subject filter to apply to the messages"""

    lag: int
    """How many messages behind the mirror operation is"""

    name: str
    """The name of the Stream being replicated"""

    subject_transforms: NotRequired[Any]
    """The subject filtering sources and associated destination transforms"""


class LostStreamData(TypedDict):
    """Records messages that were damaged and unrecoverable"""

    bytes: NotRequired[int]
    """The number of bytes that were lost"""

    msgs: NotRequired[Any]
    """The messages that were lost"""


class Placement(TypedDict):
    """Placement requirements for a Stream or asset leader"""

    cluster: NotRequired[str]
    """The desired cluster name"""

    preferred: NotRequired[str]
    """A preferred server name to move the leader to"""

    tags: NotRequired[list[str]]
    """Tags required on servers hosting this stream or leader"""


class PeerInfo(TypedDict):
    active: float
    """Nanoseconds since this peer was last seen"""

    current: bool
    """Indicates if the server is up to date and synchronised"""

    lag: NotRequired[int]
    """How many uncommitted operations this peer is behind the leader"""

    name: str
    """The server name of the peer"""

    observer: NotRequired[bool]
    """Indicates if the server is running as an observer only"""

    offline: NotRequired[bool]
    """Indicates the node is considered offline by the group"""


class ClusterInfo(TypedDict):
    leader: NotRequired[str]
    """The server name of the RAFT leader"""

    name: NotRequired[str]
    """The cluster name"""

    raft_group: NotRequired[str]
    """In clustered environments the name of the Raft group managing the asset"""

    replicas: NotRequired[list[PeerInfo]]
    """The members of the RAFT cluster"""


class ApiStats(TypedDict):
    errors: int
    """API requests that resulted in an error response"""

    inflight: NotRequired[int]
    """The number of inflight API requests waiting to be processed"""

    level: NotRequired[int]
    """The JetStream API Level"""

    total: int
    """Total number of API requests received for this account"""


class AccountLimits(TypedDict):
    max_ack_pending: NotRequired[int]
    """The maximum number of outstanding ACKs any consumer may configure"""

    max_bytes_required: NotRequired[bool]
    """Indicates if Streams created in this account requires the max_bytes property set"""

    max_consumers: int
    """The maximum number of Consumer an account can create"""

    max_memory: int
    """The maximum amount of Memory storage Stream Messages may consume"""

    max_storage: int
    """The maximum amount of File storage Stream Messages may consume"""

    max_streams: int
    """The maximum number of Streams an account can create"""

    memory_max_stream_bytes: NotRequired[int]
    """The maximum size any single memory stream may be"""

    storage_max_stream_bytes: NotRequired[int]
    """The maximum size any single storage based stream may be"""


class Tier(TypedDict):
    consumers: int
    """Number of active Consumers"""

    limits: AccountLimits

    memory: int
    """Memory Storage being used for Stream Message storage"""

    reserved_memory: NotRequired[int]
    """Bytes that is reserved for memory usage by this account on the server"""

    reserved_storage: NotRequired[int]
    """Bytes that is reserved for disk usage by this account on the server"""

    storage: int
    """File Storage being used for Stream Message storage"""

    streams: int
    """Number of active Streams"""


class AccountInfo(TypedDict):
    api: ApiStats

    consumers: int
    """Number of active Consumers"""

    domain: NotRequired[str]
    """The JetStream domain this account is in"""

    limits: AccountLimits

    memory: int
    """Memory Storage being used for Stream Message storage"""

    storage: int
    """File Storage being used for Stream Message storage"""

    streams: int
    """Number of active Streams"""

    tiers: NotRequired[dict[str, Tier]]


class StoredMessage(TypedDict):
    data: NotRequired[str]
    """The base64 encoded payload of the message body"""

    hdrs: NotRequired[str]
    """Base64 encoded headers for the message"""

    seq: int
    """The sequence number of the message in the Stream"""

    subject: str
    """The subject the message was originally received on"""

    time: str
    """The time the message was received"""


class IterableRequest(TypedDict):
    offset: int


class IterableResponse(TypedDict):
    limit: int

    offset: int

    total: int


class ErrorResponse(TypedDict):
    error: Error


PriorityPolicy = Literal["none", "overflow", "pinned_client"]


class StartSequenceDeliverPolicy(TypedDict):
    deliver_policy: Literal["by_start_sequence"]

    opt_start_seq: int


class AllDeliverPolicy(TypedDict):
    deliver_policy: Literal["all"]


class NewDeliverPolicy(TypedDict):
    deliver_policy: Literal["new"]


class LastDeliverPolicy(TypedDict):
    deliver_policy: Literal["last"]


class StartTimeDeliverPolicy(TypedDict):
    deliver_policy: Literal["by_start_time"]

    opt_start_time: int


class LastPerSubjectDeliverPolicy(TypedDict):
    deliver_policy: Literal["last_per_subject"]


class DeliverPolicy_AllDeliverPolicy(TypedDict):
    deliver_policy: Literal["all"]


class DeliverPolicy_LastDeliverPolicy(TypedDict):
    deliver_policy: Literal["last"]


class DeliverPolicy_NewDeliverPolicy(TypedDict):
    deliver_policy: Literal["new"]


class DeliverPolicy_StartSequenceDeliverPolicy(TypedDict):
    deliver_policy: Literal["by_start_sequence"]

    opt_start_seq: int


class DeliverPolicy_StartTimeDeliverPolicy(TypedDict):
    deliver_policy: Literal["by_start_time"]

    opt_start_time: int


class DeliverPolicy_LastPerSubjectDeliverPolicy(TypedDict):
    deliver_policy: Literal["last_per_subject"]


DeliverPolicy = Union[
    DeliverPolicy_AllDeliverPolicy,
    DeliverPolicy_LastDeliverPolicy,
    DeliverPolicy_NewDeliverPolicy,
    DeliverPolicy_StartSequenceDeliverPolicy,
    DeliverPolicy_StartTimeDeliverPolicy,
    DeliverPolicy_LastPerSubjectDeliverPolicy,
]


class StreamConsumerLimits(TypedDict):
    inactive_threshold: NotRequired[int]
    """Maximum value for inactive_threshold for consumers of this stream. Acts as a default when consumers do not set this value."""

    max_ack_pending: NotRequired[int]
    """Maximum value for max_ack_pending for consumers of this stream. Acts as a default when consumers do not set this value."""


class SequenceInfo(TypedDict):
    consumer_seq: int
    """The sequence number of the Consumer"""

    last_active: NotRequired[int]
    """The last time a message was delivered or acknowledged (for ack_floor)"""

    stream_seq: int
    """The sequence number of the Stream"""


class SequencePair(TypedDict):
    consumer_seq: int
    """The sequence number of the Consumer"""

    stream_seq: int
    """The sequence number of the Stream"""


class ConsumerConfig(TypedDict):
    ack_policy: NotRequired[Literal["none", "all", "explicit"]]
    """The requirement of client acknowledgments"""

    ack_wait: NotRequired[int]
    """How long (in nanoseconds) to allow messages to remain un-acknowledged before attempting redelivery"""

    backoff: NotRequired[list[int]]
    """List of durations in Go format that represents a retry time scale for NaK'd messages"""

    deliver_group: NotRequired[str]
    """The queue group name used to distribute messages among subscribers"""

    deliver_policy: NotRequired[Literal["all", "last", "new", "by_start_sequence", "by_start_time", "last_per_subject"]]
    """The point in the stream from which to receive messages"""

    deliver_subject: NotRequired[str]
    """The subject push consumers delivery messages to"""

    description: NotRequired[str]
    """A short description of the purpose of this consumer"""

    direct: NotRequired[bool]
    """Creates a special consumer that does not touch the Raft layers, not for general use by clients, internal use only"""

    durable_name: NotRequired[str]
    """A unique name for a durable consumer"""

    filter_subject: NotRequired[str]
    """Filter the stream by a single subjects"""

    filter_subjects: NotRequired[list[str]]
    """Filter the stream by multiple subjects"""

    flow_control: NotRequired[bool]
    """For push consumers this will regularly send an empty mess with Status header 100 and a reply subject, consumers must reply to these messages to control the rate of message delivery"""

    headers_only: NotRequired[bool]
    """Delivers only the headers of messages in the stream and not the bodies. Additionally adds Nats-Msg-Size header to indicate the size of the removed payload"""

    idle_heartbeat: NotRequired[int]
    """If the Consumer is idle for more than this many nano seconds a empty message with Status header 100 will be sent indicating the consumer is still alive"""

    inactive_threshold: NotRequired[int]
    """Duration that instructs the server to cleanup ephemeral consumers that are inactive for that long"""

    max_ack_pending: NotRequired[int]
    """The maximum number of messages without acknowledgement that can be outstanding, once this limit is reached message delivery will be suspended"""

    max_batch: NotRequired[int]
    """The largest batch property that may be specified when doing a pull on a Pull Consumer"""

    max_bytes: NotRequired[int]
    """The maximum bytes value that maybe set when dong a pull on a Pull Consumer"""

    max_deliver: NotRequired[int]
    """The number of times a message will be delivered to consumers if not acknowledged in time"""

    max_expires: NotRequired[int]
    """The maximum expires value that may be set when doing a pull on a Pull Consumer"""

    max_waiting: NotRequired[int]
    """The number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored"""

    mem_storage: NotRequired[bool]
    """Force the consumer state to be kept in memory rather than inherit the setting from the stream"""

    metadata: NotRequired[dict[str, str]]
    """Additional metadata for the Consumer"""

    name: NotRequired[str]
    """A unique name for a consumer"""

    num_replicas: NotRequired[int]
    """When set do not inherit the replica count from the stream but specifically set it to this amount"""

    opt_start_seq: NotRequired[int]
    """Start sequence used with the DeliverByStartSequence deliver policy."""

    opt_start_time: NotRequired[int]
    """Start time used with the DeliverByStartSequence deliver policy"""

    pause_until: NotRequired[int]
    """When creating a consumer supplying a time in the future will act as a deadline for when the consumer will be paused till"""

    priority_groups: NotRequired[list[str]]
    """List of priority groups this consumer supports"""

    priority_policy: NotRequired[PriorityPolicy]
    """The priority policy the consumer is set to"""

    priority_timeout: NotRequired[Any]
    """For pinned_client priority policy how long before the client times out"""

    rate_limit_bps: NotRequired[int]
    """The rate at which messages will be delivered to clients, expressed in bit per second"""

    replay_policy: NotRequired[Literal["instant", "original"]]
    """The rate at which messages will be pushed to a client"""

    sample_freq: NotRequired[str]
    """Sets the percentage of acknowledgments that should be sampled for observability"""


class ConsumerInfo(TypedDict):
    ack_floor: SequenceInfo
    """The highest contiguous acknowledged message"""

    cluster: NotRequired[ClusterInfo]

    config: ConsumerConfig

    created: int
    """The time the Consumer was created"""

    delivered: SequenceInfo
    """The last message delivered from this Consumer"""

    name: str
    """A unique name for the consumer, either machine generated or the durable name"""

    num_ack_pending: int
    """The number of messages pending acknowledgement"""

    num_pending: int
    """The number of messages left unconsumed in this Consumer"""

    num_redelivered: int
    """The number of redeliveries that have been performed"""

    num_waiting: int
    """The number of pull consumers waiting for messages"""

    pause_remaining: NotRequired[int]
    """When paused the time remaining until unpause"""

    paused: NotRequired[bool]
    """Indicates if the consumer is currently in a paused state"""

    priority_groups: NotRequired[list[PriorityGroupState]]
    """The state of Priority Groups"""

    push_bound: NotRequired[bool]
    """Indicates if any client is connected and receiving messages from a push consumer"""

    stream_name: str
    """The Stream the consumer belongs to"""

    ts: NotRequired[int]
    """The server time the consumer info was created"""


class Republish(TypedDict):
    """Rules for republishing messages from a stream with subject mapping onto new subjects for partitioning and more"""

    dest: str
    """The destination to publish to"""

    headers_only: NotRequired[bool]
    """Only send message headers, no bodies"""

    src: str
    """The source subject to republish"""


class StreamAlternate(TypedDict):
    """An alternate location to read mirrored data"""

    cluster: str
    """The name of the cluster holding the stream"""

    domain: NotRequired[str]
    """The domain holding the string"""

    name: str
    """The mirror stream name"""


class StreamInfo(TypedDict):
    alternates: NotRequired[list[StreamAlternate]]
    """List of mirrors sorted by priority"""

    cluster: NotRequired[ClusterInfo]

    config: Any
    """The active configuration for the Stream"""

    created: int
    """Timestamp when the stream was created"""

    mirror: NotRequired[StreamSourceInfo]

    sources: NotRequired[list[StreamSourceInfo]]
    """Streams being sourced into this Stream"""

    state: Any
    """Detail about the current State of the Stream"""

    ts: NotRequired[int]
    """The server time the stream info was created"""


class StreamState(TypedDict):
    bytes: int
    """Combined size of all messages in the Stream"""

    consumer_count: int
    """Number of Consumers attached to the Stream"""

    deleted: NotRequired[list[int]]
    """IDs of messages that were deleted using the Message Delete API or Interest based streams removing messages out of order"""

    first_seq: int
    """Sequence number of the first message in the Stream"""

    first_ts: NotRequired[str]
    """The timestamp of the first message in the Stream"""

    last_seq: int
    """Sequence number of the last message in the Stream"""

    last_ts: NotRequired[str]
    """The timestamp of the last message in the Stream"""

    lost: NotRequired[LostStreamData]

    messages: int
    """Number of messages stored in the Stream"""

    num_deleted: NotRequired[int]
    """The number of deleted messages"""

    num_subjects: NotRequired[int]
    """The number of unique subjects held in the stream"""

    subjects: NotRequired[dict[str, int]]
    """Subjects and their message counts when a subjects_filter was set"""


class StreamConfig(TypedDict):
    allow_direct: NotRequired[bool]
    """Allow higher performance, direct access to get individual messages"""

    allow_msg_ttl: NotRequired[bool]
    """Enables per-message TTL using headers"""

    allow_rollup_hdrs: NotRequired[bool]
    """Allows the use of the Nats-Rollup header to replace all contents of a stream, or subject in a stream, with a single new message"""

    compression: NotRequired[Literal["none", "s2"]]
    """Optional compression algorithm used for the Stream."""

    consumer_limits: NotRequired[StreamConsumerLimits]
    """Limits of certain values that consumers can set, defaults for those who don't set these settings"""

    deny_delete: NotRequired[bool]
    """Restricts the ability to delete messages from a stream via the API. Cannot be changed once set to true"""

    deny_purge: NotRequired[bool]
    """Restricts the ability to purge messages from a stream via the API. Cannot be change once set to true"""

    description: NotRequired[str]
    """A short description of the purpose of this stream"""

    discard: NotRequired[Literal["old", "new"]]
    """When a Stream reach it's limits either old messages are deleted or new ones are denied"""

    discard_new_per_subject: NotRequired[bool]
    """When discard policy is new and the stream is one with max messages per subject set, this will apply the new behavior to every subject. Essentially turning discard new from maximum number of subjects into maximum number of messages in a subject."""

    duplicate_window: NotRequired[int]
    """The time window to track duplicate messages for, expressed in nanoseconds. 0 for default"""

    first_seq: NotRequired[int]
    """A custom sequence to use for the first message in the stream"""

    max_age: int
    """Maximum age of any message in the stream, expressed in nanoseconds. 0 for unlimited."""

    max_bytes: int
    """How big the Stream may be, when the combined stream size exceeds this old messages are removed. -1 for unlimited."""

    max_consumers: int
    """How many Consumers can be defined for a given Stream. -1 for unlimited."""

    max_msg_size: NotRequired[int]
    """The largest message that will be accepted by the Stream. -1 for unlimited."""

    max_msgs: int
    """How many messages may be in a Stream, oldest messages will be removed if the Stream exceeds this size. -1 for unlimited."""

    max_msgs_per_subject: NotRequired[int]
    """For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit"""

    metadata: NotRequired[dict[str, str]]
    """Additional metadata for the Stream"""

    mirror: NotRequired[StreamSource]
    """Maintains a 1:1 mirror of another stream with name matching this property.  When a mirror is configured subjects and sources must be empty."""

    mirror_direct: NotRequired[bool]
    """Allow higher performance, direct access for mirrors as well"""

    name: NotRequired[str]
    """A unique name for the Stream, empty for Stream Templates."""

    no_ack: NotRequired[bool]
    """Disables acknowledging messages that are received by the Stream."""

    num_replicas: int
    """How many replicas to keep for each message."""

    placement: NotRequired[Placement]
    """Placement directives to consider when placing replicas of this stream, random placement when unset"""

    republish: NotRequired[Republish]

    retention: Literal["limits", "interest", "workqueue"]
    """How messages are retained in the Stream, once this is exceeded old messages are removed."""

    sealed: NotRequired[bool]
    """Sealed streams do not allow messages to be deleted via limits or API, sealed streams can not be unsealed via configuration update. Can only be set on already created streams via the Update API"""

    sources: NotRequired[list[StreamSource]]
    """List of Stream names to replicate into this Stream"""

    storage: Literal["file", "memory"]
    """The storage backend to use for the Stream."""

    subject_delete_marker_ttl: NotRequired[int]
    """Enables and sets a duration for adding server markers for delete, purge and max age limits"""

    subject_transform: NotRequired[SubjectTransform]
    """Subject transform to apply to matching messages"""

    subjects: NotRequired[list[str]]
    """A list of subjects to consume, supports wildcards. Must be empty when a mirror is configured. May be empty when sources are configured."""

    template_owner: NotRequired[str]
    """When the Stream is managed by a Stream Template this identifies the template that manages the Stream."""


class StreamTemplateConfig(TypedDict):
    config: StreamConfig
    """The template configuration to create Streams with"""

    max_streams: int
    """The maximum number of streams to allow using this Template"""

    name: str
    """A unique name for the Template"""


class StreamTemplateInfo(TypedDict):
    config: StreamTemplateConfig

    streams: list[str]
    """List of Streams managed by this Template"""


class ConsumerInfoResponse(TypedDict):
    """A response from the JetStream $JS.API.CONSUMER.INFO API"""

    ack_floor: SequenceInfo
    """The highest contiguous acknowledged message"""

    cluster: NotRequired[ClusterInfo]

    config: ConsumerConfig

    created: int
    """The time the Consumer was created"""

    delivered: SequenceInfo
    """The last message delivered from this Consumer"""

    name: str
    """A unique name for the consumer, either machine generated or the durable name"""

    num_ack_pending: int
    """The number of messages pending acknowledgement"""

    num_pending: int
    """The number of messages left unconsumed in this Consumer"""

    num_redelivered: int
    """The number of redeliveries that have been performed"""

    num_waiting: int
    """The number of pull consumers waiting for messages"""

    pause_remaining: NotRequired[int]
    """When paused the time remaining until unpause"""

    paused: NotRequired[bool]
    """Indicates if the consumer is currently in a paused state"""

    priority_groups: NotRequired[list[PriorityGroupState]]
    """The state of Priority Groups"""

    push_bound: NotRequired[bool]
    """Indicates if any client is connected and receiving messages from a push consumer"""

    stream_name: str
    """The Stream the consumer belongs to"""

    ts: NotRequired[int]
    """The server time the consumer info was created"""

    type: Literal["io.nats.jetstream.api.v1.consumer_info_response"]


class StreamPurgeResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.PURGE API"""

    purged: int
    """Number of messages purged from the Stream"""

    success: bool

    type: Literal["io.nats.jetstream.api.v1.stream_purge_response"]


class ConsumerUnpinRequest(TypedDict):
    """A request to the JetStream $JS.API.CONSUMER.UNPIN API"""

    group: str
    """The group to unpin"""


class ConsumerDeleteResponse(TypedDict):
    """A response from the JetStream $JS.API.CONSUMER.DELETE API"""

    success: bool

    type: Literal["io.nats.jetstream.api.v1.consumer_delete_response"]


class ConsumerGetnextRequest(TypedDict):
    """A request to the JetStream $JS.API.CONSUMER.MSG.NEXT API"""

    batch: NotRequired[int]
    """How many messages the server should deliver to the requester"""

    expires: NotRequired[int]
    """A duration from now when the pull should expire, stated in nanoseconds, 0 for no expiry"""

    idle_heartbeat: NotRequired[int]
    """When not 0 idle heartbeats will be sent on this interval"""

    max_bytes: NotRequired[int]
    """Sends at most this many bytes to the requester, limited by consumer configuration max_bytes"""

    no_wait: NotRequired[bool]
    """When true a response with a 404 status header will be returned when no messages are available"""


class StreamCreateRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.CREATE API"""

    allow_direct: NotRequired[bool]
    """Allow higher performance, direct access to get individual messages"""

    allow_msg_ttl: NotRequired[bool]
    """Enables per-message TTL using headers"""

    allow_rollup_hdrs: NotRequired[bool]
    """Allows the use of the Nats-Rollup header to replace all contents of a stream, or subject in a stream, with a single new message"""

    compression: NotRequired[Literal["none", "s2"]]
    """Optional compression algorithm used for the Stream."""

    consumer_limits: NotRequired[StreamConsumerLimits]
    """Limits of certain values that consumers can set, defaults for those who don't set these settings"""

    deny_delete: NotRequired[bool]
    """Restricts the ability to delete messages from a stream via the API. Cannot be changed once set to true"""

    deny_purge: NotRequired[bool]
    """Restricts the ability to purge messages from a stream via the API. Cannot be change once set to true"""

    description: NotRequired[str]
    """A short description of the purpose of this stream"""

    discard: NotRequired[Literal["old", "new"]]
    """When a Stream reach it's limits either old messages are deleted or new ones are denied"""

    discard_new_per_subject: NotRequired[bool]
    """When discard policy is new and the stream is one with max messages per subject set, this will apply the new behavior to every subject. Essentially turning discard new from maximum number of subjects into maximum number of messages in a subject."""

    duplicate_window: NotRequired[int]
    """The time window to track duplicate messages for, expressed in nanoseconds. 0 for default"""

    first_seq: NotRequired[int]
    """A custom sequence to use for the first message in the stream"""

    max_age: int
    """Maximum age of any message in the stream, expressed in nanoseconds. 0 for unlimited."""

    max_bytes: int
    """How big the Stream may be, when the combined stream size exceeds this old messages are removed. -1 for unlimited."""

    max_consumers: int
    """How many Consumers can be defined for a given Stream. -1 for unlimited."""

    max_msg_size: NotRequired[int]
    """The largest message that will be accepted by the Stream. -1 for unlimited."""

    max_msgs: int
    """How many messages may be in a Stream, oldest messages will be removed if the Stream exceeds this size. -1 for unlimited."""

    max_msgs_per_subject: NotRequired[int]
    """For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit"""

    metadata: NotRequired[dict[str, str]]
    """Additional metadata for the Stream"""

    mirror: NotRequired[StreamSource]
    """Maintains a 1:1 mirror of another stream with name matching this property.  When a mirror is configured subjects and sources must be empty."""

    mirror_direct: NotRequired[bool]
    """Allow higher performance, direct access for mirrors as well"""

    name: NotRequired[str]
    """A unique name for the Stream, empty for Stream Templates."""

    no_ack: NotRequired[bool]
    """Disables acknowledging messages that are received by the Stream."""

    num_replicas: int
    """How many replicas to keep for each message."""

    pedantic: NotRequired[bool]
    """Enables pedantic mode where the server will not apply defaults or change the request"""

    placement: NotRequired[Placement]
    """Placement directives to consider when placing replicas of this stream, random placement when unset"""

    republish: NotRequired[Republish]

    retention: Literal["limits", "interest", "workqueue"]
    """How messages are retained in the Stream, once this is exceeded old messages are removed."""

    sealed: NotRequired[bool]
    """Sealed streams do not allow messages to be deleted via limits or API, sealed streams can not be unsealed via configuration update. Can only be set on already created streams via the Update API"""

    sources: NotRequired[list[StreamSource]]
    """List of Stream names to replicate into this Stream"""

    storage: Literal["file", "memory"]
    """The storage backend to use for the Stream."""

    subject_delete_marker_ttl: NotRequired[int]
    """Enables and sets a duration for adding server markers for delete, purge and max age limits"""

    subject_transform: NotRequired[SubjectTransform]
    """Subject transform to apply to matching messages"""

    subjects: NotRequired[list[str]]
    """A list of subjects to consume, supports wildcards. Must be empty when a mirror is configured. May be empty when sources are configured."""

    template_owner: NotRequired[str]
    """When the Stream is managed by a Stream Template this identifies the template that manages the Stream."""


class ConsumerNamesResponse(TypedDict):
    """A response from the JetStream $JS.API.CONSUMER.NAMES API"""

    consumers: list[str]

    limit: int

    offset: int

    total: int

    type: Literal["io.nats.jetstream.api.v1.consumer_names_response"]


class StreamCreateResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.CREATE API"""

    alternates: NotRequired[list[StreamAlternate]]
    """List of mirrors sorted by priority"""

    cluster: NotRequired[ClusterInfo]

    config: Any
    """The active configuration for the Stream"""

    created: int
    """Timestamp when the stream was created"""

    mirror: NotRequired[StreamSourceInfo]

    sources: NotRequired[list[StreamSourceInfo]]
    """Streams being sourced into this Stream"""

    state: Any
    """Detail about the current State of the Stream"""

    ts: NotRequired[int]
    """The server time the stream info was created"""

    type: Literal["io.nats.jetstream.api.v1.stream_create_response"]


class ConsumerListRequest(TypedDict):
    """A request to the JetStream $JS.API.CONSUMER.LIST API"""

    offset: int


class StreamTemplateCreateResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.TEMPLATE.CREATE API"""

    config: StreamTemplateConfig

    streams: list[str]
    """List of Streams managed by this Template"""

    type: Literal["io.nats.jetstream.api.v1.stream_template_create_response"]


class MetaLeaderStepdownResponse(TypedDict):
    """A response from the JetStream $JS.API.META.LEADER.STEPDOWN API"""

    success: bool
    """If the leader successfully stood down"""

    type: NotRequired[Literal["io.nats.jetstream.api.v1.meta_leader_stepdown_response"]]


class MetaServerRemoveRequest(TypedDict):
    """A request to the JetStream $JS.API.SERVER.REMOVE API"""

    peer: NotRequired[str]
    """The Name of the server to remove from the meta group"""

    peer_id: NotRequired[str]
    """Peer ID of the peer to be removed. If specified this is used instead of the server name"""


class StreamPurgeRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.PURGE API"""

    filter: NotRequired[str]
    """Restrict purging to messages that match this subject"""

    keep: NotRequired[int]
    """Ensures this many messages are present after the purge. Can be combined with the subject filter but not the sequence"""

    seq: NotRequired[int]
    """Purge all messages up to but not including the message with this sequence. Can be combined with subject filter but not the keep option"""


class StreamUpdateRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.UPDATE API"""

    allow_direct: NotRequired[bool]
    """Allow higher performance, direct access to get individual messages"""

    allow_msg_ttl: NotRequired[bool]
    """Enables per-message TTL using headers"""

    allow_rollup_hdrs: NotRequired[bool]
    """Allows the use of the Nats-Rollup header to replace all contents of a stream, or subject in a stream, with a single new message"""

    compression: NotRequired[Literal["none", "s2"]]
    """Optional compression algorithm used for the Stream."""

    consumer_limits: NotRequired[StreamConsumerLimits]
    """Limits of certain values that consumers can set, defaults for those who don't set these settings"""

    deny_delete: NotRequired[bool]
    """Restricts the ability to delete messages from a stream via the API. Cannot be changed once set to true"""

    deny_purge: NotRequired[bool]
    """Restricts the ability to purge messages from a stream via the API. Cannot be change once set to true"""

    description: NotRequired[str]
    """A short description of the purpose of this stream"""

    discard: NotRequired[Literal["old", "new"]]
    """When a Stream reach it's limits either old messages are deleted or new ones are denied"""

    discard_new_per_subject: NotRequired[bool]
    """When discard policy is new and the stream is one with max messages per subject set, this will apply the new behavior to every subject. Essentially turning discard new from maximum number of subjects into maximum number of messages in a subject."""

    duplicate_window: NotRequired[int]
    """The time window to track duplicate messages for, expressed in nanoseconds. 0 for default"""

    first_seq: NotRequired[int]
    """A custom sequence to use for the first message in the stream"""

    max_age: int
    """Maximum age of any message in the stream, expressed in nanoseconds. 0 for unlimited."""

    max_bytes: int
    """How big the Stream may be, when the combined stream size exceeds this old messages are removed. -1 for unlimited."""

    max_consumers: int
    """How many Consumers can be defined for a given Stream. -1 for unlimited."""

    max_msg_size: NotRequired[int]
    """The largest message that will be accepted by the Stream. -1 for unlimited."""

    max_msgs: int
    """How many messages may be in a Stream, oldest messages will be removed if the Stream exceeds this size. -1 for unlimited."""

    max_msgs_per_subject: NotRequired[int]
    """For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit"""

    metadata: NotRequired[dict[str, str]]
    """Additional metadata for the Stream"""

    mirror: NotRequired[StreamSource]
    """Maintains a 1:1 mirror of another stream with name matching this property.  When a mirror is configured subjects and sources must be empty."""

    mirror_direct: NotRequired[bool]
    """Allow higher performance, direct access for mirrors as well"""

    name: NotRequired[str]
    """A unique name for the Stream, empty for Stream Templates."""

    no_ack: NotRequired[bool]
    """Disables acknowledging messages that are received by the Stream."""

    num_replicas: int
    """How many replicas to keep for each message."""

    placement: NotRequired[Placement]
    """Placement directives to consider when placing replicas of this stream, random placement when unset"""

    republish: NotRequired[Republish]

    retention: Literal["limits", "interest", "workqueue"]
    """How messages are retained in the Stream, once this is exceeded old messages are removed."""

    sealed: NotRequired[bool]
    """Sealed streams do not allow messages to be deleted via limits or API, sealed streams can not be unsealed via configuration update. Can only be set on already created streams via the Update API"""

    sources: NotRequired[list[StreamSource]]
    """List of Stream names to replicate into this Stream"""

    storage: Literal["file", "memory"]
    """The storage backend to use for the Stream."""

    subject_delete_marker_ttl: NotRequired[int]
    """Enables and sets a duration for adding server markers for delete, purge and max age limits"""

    subject_transform: NotRequired[SubjectTransform]
    """Subject transform to apply to matching messages"""

    subjects: NotRequired[list[str]]
    """A list of subjects to consume, supports wildcards. Must be empty when a mirror is configured. May be empty when sources are configured."""

    template_owner: NotRequired[str]
    """When the Stream is managed by a Stream Template this identifies the template that manages the Stream."""


class StreamSnapshotResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.SNAPSHOT API"""

    config: StreamConfig

    state: StreamState

    type: NotRequired[Literal["io.nats.jetstream.api.v1.stream_snapshot_response"]]


class StreamRemovePeerResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.PEER.REMOVE API"""

    success: bool
    """If the peer was successfully removed"""

    type: NotRequired[Literal["io.nats.jetstream.api.v1.stream_remove_peer_response"]]


class StreamMsgDeleteResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.MSG.DELETE API"""

    success: bool

    type: Literal["io.nats.jetstream.api.v1.stream_msg_delete_response"]


class StreamTemplateDeleteResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.TEMPLATE.DELETE API"""

    success: bool

    type: Literal["io.nats.jetstream.api.v1.stream_template_delete_response"]


class StreamMsgDeleteRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.MSG.DELETE API"""

    no_erase: NotRequired[bool]
    """Default will securely remove a message and rewrite the data with random data, set this to true to only remove the message"""

    seq: int
    """Stream sequence number of the message to delete"""


class StreamDeleteResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.DELETE API"""

    success: bool

    type: Literal["io.nats.jetstream.api.v1.stream_delete_response"]


class ConsumerPauseResponse(TypedDict):
    """A response from the JetStream $JS.API.CONSUMER.PAUSE API"""

    pause_remaining: NotRequired[int]
    """When paused the time remaining until unpause"""

    pause_until: NotRequired[int]
    """The deadline till the consumer will be unpaused, only usable if 'paused' is true"""

    paused: NotRequired[bool]
    """Indicates if after parsing the pause_until property if the consumer was paused"""

    type: NotRequired[Literal["io.nats.jetstream.api.v1.consumer_pause_response"]]


class StreamTemplateNamesResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.TEMPLATE.NAMES API"""

    consumers: NotRequired[list[str]]

    limit: int

    offset: int

    total: int

    type: Literal["io.nats.jetstream.api.v1.stream_template_names_response"]


class StreamTemplateInfoResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.TEMPLATE.INFO API"""

    config: StreamTemplateConfig

    streams: list[str]
    """List of Streams managed by this Template"""

    type: Literal["io.nats.jetstream.api.v1.stream_template_info_response"]


class StreamLeaderStepdownRequest(TypedDict):
    """A request for the JetStream $JS.API.STREAM.LEADER.STEPDOWN API"""

    placement: NotRequired[Placement]
    """Placement directives to consider when placing the leader"""


class StreamInfoResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.INFO API"""

    alternates: NotRequired[list[StreamAlternate]]
    """List of mirrors sorted by priority"""

    cluster: NotRequired[ClusterInfo]

    config: Any
    """The active configuration for the Stream"""

    created: int
    """Timestamp when the stream was created"""

    limit: NotRequired[int]

    mirror: NotRequired[StreamSourceInfo]

    offset: NotRequired[int]

    sources: NotRequired[list[StreamSourceInfo]]
    """Streams being sourced into this Stream"""

    state: Any
    """Detail about the current State of the Stream"""

    total: NotRequired[int]

    ts: NotRequired[int]
    """The server time the stream info was created"""

    type: Literal["io.nats.jetstream.api.v1.stream_info_response"]


class ConsumerCreateResponse(TypedDict):
    """A response from the JetStream $JS.API.CONSUMER.CREATE API"""

    ack_floor: SequenceInfo
    """The highest contiguous acknowledged message"""

    cluster: NotRequired[ClusterInfo]

    config: ConsumerConfig

    created: int
    """The time the Consumer was created"""

    delivered: SequenceInfo
    """The last message delivered from this Consumer"""

    name: str
    """A unique name for the consumer, either machine generated or the durable name"""

    num_ack_pending: int
    """The number of messages pending acknowledgement"""

    num_pending: int
    """The number of messages left unconsumed in this Consumer"""

    num_redelivered: int
    """The number of redeliveries that have been performed"""

    num_waiting: int
    """The number of pull consumers waiting for messages"""

    pause_remaining: NotRequired[int]
    """When paused the time remaining until unpause"""

    paused: NotRequired[bool]
    """Indicates if the consumer is currently in a paused state"""

    priority_groups: NotRequired[list[PriorityGroupState]]
    """The state of Priority Groups"""

    push_bound: NotRequired[bool]
    """Indicates if any client is connected and receiving messages from a push consumer"""

    stream_name: str
    """The Stream the consumer belongs to"""

    ts: NotRequired[int]
    """The server time the consumer info was created"""

    type: Literal["io.nats.jetstream.api.v1.consumer_create_response"]


class StreamRemovePeerRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.PEER.REMOVE API"""

    peer: str
    """Server name of the peer to remove"""


class StreamSnapshotRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.SNAPSHOT API"""

    chunk_size: NotRequired[int]
    """The size of data chunks to send to deliver_subject"""

    deliver_subject: str
    """The NATS subject where the snapshot will be delivered"""

    jsck: NotRequired[bool]
    """Check all message's checksums prior to snapshot"""

    no_consumers: NotRequired[bool]
    """When true consumer states and configurations will not be present in the snapshot"""


class StreamListResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.LIST API"""

    limit: int

    missing: NotRequired[list[str]]
    """In clustered environments gathering Stream info might time out, this list would be a list of Streams for which information was not obtainable"""

    offset: int

    streams: list[StreamInfo]
    """Full Stream information for each known Stream"""

    total: int

    type: Literal["io.nats.jetstream.api.v1.stream_list_response"]


class ConsumerCreateRequest(TypedDict):
    """A request to the JetStream $JS.API.CONSUMER.CREATE API"""

    action: NotRequired[str]
    """The consumer create action"""

    config: ConsumerConfig
    """The consumer configuration"""

    pedantic: NotRequired[bool]
    """Enables pedantic mode where the server will not apply defaults or change the request"""

    stream_name: str
    """The name of the stream to create the consumer in"""


class StreamInfoRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.INFO API"""

    deleted_details: NotRequired[bool]
    """When true will result in a full list of deleted message IDs being returned in the info response"""

    offset: NotRequired[int]
    """Paging offset when retrieving pages of subject details"""

    subjects_filter: NotRequired[str]
    """When set will return a list of subjects and how many messages they hold for all matching subjects. Filter is a standard NATS subject wildcard pattern."""


class ConsumerNamesRequest(TypedDict):
    """A request to the JetStream $JS.API.CONSUMER.NAMES API"""

    offset: int

    subject: NotRequired[str]
    """Filter the names to those consuming messages matching this subject or wildcard"""


class StreamRestoreResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.RESTORE API"""

    deliver_subject: str
    """The Subject to send restore chunks to"""

    type: NotRequired[Literal["io.nats.jetstream.api.v1.stream_restore_response"]]


class StreamListRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.LIST API"""

    offset: NotRequired[int]

    subject: NotRequired[str]
    """Limit the list to streams matching this subject filter"""


class MetaServerRemoveResponse(TypedDict):
    """A response from the JetStream $JS.API.SERVER.REMOVE API"""

    success: bool
    """If the peer was successfully removed"""

    type: NotRequired[Literal["io.nats.jetstream.api.v1.meta_server_remove_response"]]


class AccountInfoResponse(TypedDict):
    """A response from the JetStream $JS.API.INFO API"""

    api: ApiStats

    consumers: int
    """Number of active Consumers"""

    domain: NotRequired[str]
    """The JetStream domain this account is in"""

    limits: AccountLimits

    memory: int
    """Memory Storage being used for Stream Message storage"""

    storage: int
    """File Storage being used for Stream Message storage"""

    streams: int
    """Number of active Streams"""

    tiers: NotRequired[dict[str, Tier]]

    type: Literal["io.nats.jetstream.api.v1.account_info_response"]


class PublishAck(TypedDict):
    """A response received when publishing a message"""

    domain: NotRequired[str]
    """If the Stream accepting the message is in a JetStream server configured for a domain this would be that domain"""

    duplicate: NotRequired[bool]
    """Indicates that the message was not stored due to the Nats-Msg-Id header and duplicate tracking"""

    error: NotRequired[Error]

    seq: NotRequired[int]
    """If successful this will be the sequence the message is stored at"""

    stream: str
    """The name of the stream that received the message"""


class AccountPurgeResponse(TypedDict):
    """A response from the JetStream $JS.API.ACCOUNT.PURGE API"""

    initiated: NotRequired[bool]
    """If the purge operation was successfully started"""

    type: NotRequired[Literal["io.nats.jetstream.api.v1.account_purge_response"]]


class StreamNamesResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.NAMES API"""

    consumers: NotRequired[list[str]]

    limit: int

    offset: int

    total: int

    type: Literal["io.nats.jetstream.api.v1.stream_names_response"]


class StreamUpdateResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.UPDATE API"""

    alternates: NotRequired[list[StreamAlternate]]
    """List of mirrors sorted by priority"""

    cluster: NotRequired[ClusterInfo]

    config: Any
    """The active configuration for the Stream"""

    created: int
    """Timestamp when the stream was created"""

    mirror: NotRequired[StreamSourceInfo]

    sources: NotRequired[list[StreamSourceInfo]]
    """Streams being sourced into this Stream"""

    state: Any
    """Detail about the current State of the Stream"""

    ts: NotRequired[int]
    """The server time the stream info was created"""

    type: Literal["io.nats.jetstream.api.v1.stream_update_response"]


class ConsumerLeaderStepdownResponse(TypedDict):
    """A response from the JetStream $JS.API.CONSUMER.LEADER.STEPDOWN API"""

    success: bool
    """If the leader successfully stood down"""

    type: NotRequired[Literal["io.nats.jetstream.api.v1.consumer_leader_stepdown_response"]]


class StreamNamesRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.NAMES API"""

    offset: NotRequired[int]

    subject: NotRequired[str]
    """Limit the list to streams matching this subject filter"""


class ConsumerUnpinResponse(TypedDict):
    """A response from the JetStream $JS.API.CONSUMER.UNPIN API"""

    error: NotRequired[ErrorResponse]

    type: Literal["io.nats.jetstream.api.v1.consumer_pause_response"]


class MetaLeaderStepdownRequest(TypedDict):
    """A request to the JetStream $JS.API.META.LEADER.STEPDOWN API"""

    placement: NotRequired[Placement]


class ConsumerPauseRequest(TypedDict):
    """A request to the JetStream $JS.API.CONSUMER.PAUSE API"""

    pause_until: NotRequired[int]
    """Time to pause until, when empty or a time in the past will unpause the consumer"""


class StreamRestoreRequest(TypedDict):
    """A response from the JetStream $JS.API.STREAM.RESTORE API"""

    config: StreamConfig

    state: StreamState


class StreamLeaderStepdownResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.LEADER.STEPDOWN API"""

    success: bool
    """If the leader successfully stood down"""

    type: NotRequired[Literal["io.nats.jetstream.api.v1.stream_leader_stepdown_response"]]


class StreamMsgGetRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.MSG.GET API"""

    batch: NotRequired[int]
    """Request a number of messages to be delivered"""

    last_by_subj: NotRequired[str]
    """Retrieves the last message for a given subject, cannot be combined with seq"""

    max_bytes: NotRequired[int]
    """Restrict batch get to a certain maximum cumulative bytes, defaults to server MAX_PENDING_SIZE"""

    multi_last: NotRequired[list[str]]
    """Get the last messages from the supplied subjects"""

    next_by_subj: NotRequired[str]
    """Combined with sequence gets the next message for a subject with the given sequence or higher"""

    seq: NotRequired[int]
    """Stream sequence number of the message to retrieve, cannot be combined with last_by_subj"""

    start_time: NotRequired[int]
    """Start the batch at a certain point in time rather than sequence"""

    up_to_seq: NotRequired[int]
    """Returns messages up to this sequence otherwise last sequence for the stream"""

    up_to_time: NotRequired[int]
    """Only return messages up to a point in time"""


class ConsumerListResponse(TypedDict):
    """A response from the JetStream $JS.API.CONSUMER.LIST API"""

    consumers: list[ConsumerInfo]
    """Full Consumer information for each known Consumer"""

    limit: int

    offset: int

    total: int

    type: Literal["io.nats.jetstream.api.v1.consumer_list_response"]


class StreamMsgGetResponse(TypedDict):
    """A response from the JetStream $JS.API.STREAM.MSG.GET API"""

    message: StoredMessage

    type: Literal["io.nats.jetstream.api.v1.stream_msg_get_response"]


class StreamTemplateCreateRequest(TypedDict):
    """A request to the JetStream $JS.API.STREAM.TEMPLATE.CREATE API"""

    config: StreamConfig
    """The template configuration to create Streams with"""

    max_streams: int
    """The maximum number of streams to allow using this Template"""

    name: str
    """A unique name for the Template"""


class ConsumerLeaderStepdownRequest(TypedDict):
    """A request for the JetStream $JS.API.CONSUMER.LEADER.STEPDOWN API"""

    placement: NotRequired[Placement]
    """Placement directives to consider when placing the leader"""


class StreamTemplateNamesRequest(TypedDict):
    """A request to the JetStream $JS.API.CONSUMER.LIST API"""

    offset: int
