from __future__ import annotations

import random
import hashlib
import string

from datetime import datetime
from enum import Enum
from typing import AsyncIterable, Optional, Literal, List, Protocol, Dict, Any, AsyncIterator, AsyncIterable
from dataclasses import dataclass, field

from nats.jetstream.api import CONSUMER_NOT_FOUND, Client, JetStreamError

CONSUMER_CREATE_ACTION  = "create"
CONSUMER_UPDATE_ACTION = "update"
CONSUMER_CREATE_OR_UPDATE_ACTION = ""

class DeliverPolicy(Enum):
    """
    DeliverPolicy determines from which point to start delivering messages.
    """
    ALL = "all"
    """DeliverAllPolicy starts delivering messages from the very beginning of a stream."""

    LAST = "last"
    """DeliverLastPolicy will start the consumer with the last sequence received."""

    NEW = "new"
    """DeliverNewPolicy will only deliver new messages that are sent after the consumer is created."""

    BY_START_SEQUENCE = "by_start_sequence"
    """DeliverByStartSequencePolicy will deliver messages starting from a given sequence configured with OptStartSeq."""

    BY_START_TIME = "by_start_time"
    """DeliverByStartTimePolicy will deliver messages starting from a given time configured with OptStartTime."""

    LAST_PER_SUBJECT = "last_per_subject"
    """DeliverLastPerSubjectPolicy will start the consumer with the last message for all subjects received."""

class AckPolicy(Enum):
    """
    AckPolicy determines how the consumer should acknowledge delivered messages.
    """
    NONE = "none"
    """AckNonePolicy requires no acks for delivered messages."""

    ALL = "all"
    """AckAllPolicy when acking a sequence number, this implicitly acks all sequences below this one as well."""

    EXPLICIT = "explicit"
    """AckExplicitPolicy requires ack or nack for all messages."""


class ReplayPolicy(Enum):
    """
    ReplayPolicy determines how the consumer should replay messages it
    already has queued in the stream.
    """
    INSTANT = "instant"
    """ReplayInstantPolicy will replay messages as fast as possible."""

    ORIGINAL = "original"
    """ReplayOriginalPolicy will maintain the same timing as the messages were received."""


@dataclass
class SequenceInfo:
    """
    SequenceInfo has both the consumer and the stream sequence and last activity.
    """
    consumer: int
    """Consumer sequence number."""

    stream: int
    """Stream sequence number."""

    last_active: Optional[datetime] = None
    """Last activity timestamp."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SequenceInfo:
        return cls(
            consumer=data['consumer_seq'],
            stream=data['stream_seq'],
            last_active=datetime.fromtimestamp(data['last_active']) if data.get('last_active') else None
        )

@dataclass
class PeerInfo:
    """
    PeerInfo shows information about the peers in the cluster that are
    supporting the stream or consumer.
    """

    name: str
    """The server name of the peer."""

    current: bool
    """Indicates if the peer is up to date and synchronized with the leader."""

    active: int
    """The duration since this peer was last seen."""

    offline: Optional[bool] = None
    """Indicates if the peer is considered offline by the group."""

    lag: Optional[int] = None
    """The number of uncommitted operations this peer is behind the leader."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PeerInfo:
        return cls(
            name=data['name'],
            current=data['current'],
            active=data['active'],
            offline=data.get('offline', None),
            lag=data.get('lag', None)
        )

@dataclass
class ClusterInfo:
    """
    ClusterInfo shows information about the underlying set of servers that
    make up the stream or consumer.
    """

    name: Optional[str] = None
    """Name is the name of the cluster."""

    leader: Optional[str] = None
    """Leader is the server name of the RAFT leader."""

    replicas: List[PeerInfo] = field(
        default_factory=list
    )
    """Replicas is the list of members of the RAFT cluster."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ClusterInfo:
        return cls(
            name=data.get('name'),
            leader=data.get('leader'),
            replicas=[PeerInfo.from_dict(replica) for replica in data.get('replicas', [])]
        )

@dataclass
class ConsumerConfig:
    """
    ConsumerConfig is the configuration of a JetStream consumer.
    """
    name: Optional[str] = None
    """Optional name for the consumer."""

    durable: Optional[str] = None
    """Optional durable name for the consumer."""

    description: Optional[str] = None
    """Optional description of the consumer."""

    deliver_policy: Optional[DeliverPolicy] = None
    """Defines from which point to start delivering messages from the stream. Defaults to DeliverAllPolicy."""

    opt_start_seq: Optional[int] = None
    """Optional sequence number from which to start message delivery."""

    opt_start_time: Optional[datetime] = None
    """Optional time from which to start message delivery."""

    ack_policy: Optional[AckPolicy] = None
    """Defines the acknowledgement policy for the consumer. Defaults to AckExplicitPolicy."""

    ack_wait: Optional[int] = None
    """How long the server will wait for an acknowledgement before resending a message."""

    max_deliver: Optional[int] = None
    """Maximum number of delivery attempts for a message."""

    backoff: Optional[List[int]] = None
    """Optional back-off intervals for retrying message delivery after a failed acknowledgement."""

    filter_subject: Optional[str] = None
    """Can be used to filter messages delivered from the stream."""

    replay_policy: Optional[ReplayPolicy] = None
    """Defines the rate at which messages are sent to the consumer."""

    rate_limit: Optional[int] = None
    """Optional maximum rate of message delivery in bits per second."""

    sample_frequency: Optional[str] = None
    """Optional frequency for sampling how often acknowledgements are sampled for observability."""

    max_waiting: Optional[int] = None
    """Maximum number of pull requests waiting to be fulfilled."""

    max_ack_pending: Optional[int] = None
    """Maximum number of outstanding unacknowledged messages."""

    headers_only: Optional[bool] = None
    """Indicates whether only headers of messages should be sent."""

    max_request_batch: Optional[int] = None
    """Optional maximum batch size a single pull request can make."""

    max_request_expires: Optional[int] = None
    """Maximum duration a single pull request will wait for messages to be available to pull."""

    max_request_max_bytes: Optional[int] = None
    """Optional maximum total bytes that can be requested in a given batch."""

    inactive_threshold: Optional[int] = None
    """Duration which instructs the server to clean up the consumer if it has been inactive."""

    replicas: Optional[int] = None
    """Number of replicas for the consumer's state."""

    memory_storage: Optional[bool] = None
    """Flag to force the consumer to use memory storage."""

    filter_subjects: Optional[List[str]] = None
    """Allows filtering messages from a stream by subject."""

    metadata: Optional[Dict[str, str]] = None
    """Set of application-defined key-value pairs for associating metadata on the consumer."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ConsumerConfig:
        return cls(
            name=data.get('name'),
            durable=data.get('durable'),
            description=data.get('description'),
            deliver_policy=DeliverPolicy(data.get('deliver_policy')) if data.get('deliver_policy') else None,
            opt_start_seq=data.get('opt_start_seq'),
            opt_start_time=datetime.fromisoformat(data['opt_start_time']) if data.get('opt_start_time') else None,
            ack_policy=AckPolicy(data.get('ack_policy')) if data.get('ack_policy') else None,
            ack_wait=data.get('ack_wait'),
            max_deliver=data.get('max_deliver'),
            backoff=data.get('backoff'),
            filter_subject=data.get('filter_subject'),
            replay_policy=ReplayPolicy(data.get('replay_policy')) if data.get('replay_policy') else None,
            rate_limit=data.get('rate_limit'),
            sample_frequency=data.get('sample_frequency'),
            max_waiting=data.get('max_waiting'),
            max_ack_pending=data.get('max_ack_pending'),
            headers_only=data.get('headers_only'),
            max_request_batch=data.get('max_request_batch'),
            max_request_expires=data.get('max_request_expires'),
            max_request_max_bytes=data.get('max_request_max_bytes'),
            inactive_threshold=data.get('inactive_threshold'),
            replicas=data.get('replicas'),
            memory_storage=data.get('memory_storage'),
            filter_subjects=data.get('filter_subjects'),
            metadata=data.get('metadata')
        )

    def to_dict(self) -> Dict[str, Any]:
        return {key: value for key, value in {
            'name': self.name,
            'durable_name': self.durable,
            'description': self.description,
            'deliver_policy': self.deliver_policy,
            'opt_start_seq': self.opt_start_seq,
            'opt_start_time': self.opt_start_time,
            'ack_policy': self.ack_policy.value if self.ack_policy else None,
            'ack_wait': self.ack_wait,
            'max_deliver': self.max_deliver,
            'backoff': self.backoff,
            'filter_subject': self.filter_subject,
            'replay_policy': self.replay_policy,
            'rate_limit': self.rate_limit,
            'sample_frequency': self.sample_frequency,
            'max_waiting': self.max_waiting,
            'max_ack_pending': self.max_ack_pending,
            'headers_only': self.headers_only,
            'max_request_batch': self.max_request_batch,
            'max_request_expires': self.max_request_expires,
            'max_request_max_bytes': self.max_request_max_bytes,
            'inactive_threshold': self.inactive_threshold,
            'replicas': self.replicas,
            'memory_storage': self.memory_storage,
            'filter_subjects': self.filter_subjects,
            'metadata': self.metadata
        }.items() if value is not None}


@dataclass
class ConsumerInfo:
    """
    ConsumerInfo is the detailed information about a JetStream consumer.
    """
    name: str
    """Unique identifier for the consumer."""

    stream_name: str
    """Name of the stream that the consumer is bound to."""

    created: datetime
    """Timestamp when the consumer was created."""

    config: ConsumerConfig
    """Configuration settings of the consumer."""

    delivered: SequenceInfo
    """Information about the most recently delivered message."""

    ack_floor: SequenceInfo
    """Indicates the message before the first unacknowledged message."""

    num_ack_pending: int
    """Number of messages that have been delivered but not yet acknowledged."""

    num_redelivered: int
    """Counts the number of messages that have been redelivered and not yet acknowledged."""

    num_waiting: int
    """Count of active pull requests."""

    num_pending: int
    """Number of messages that match the consumer's filter but have not been delivered yet."""

    timestamp: datetime
    """Timestamp when the info was gathered by the server."""

    push_bound: bool
    """Indicates whether at least one subscription exists for the delivery subject of this consumer."""

    cluster: Optional[ClusterInfo] = None
    """Information about the cluster to which this consumer belongs."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ConsumerInfo:
        return cls(
            name=data['name'],
            stream_name=data['stream_name'],
            created=data['created'],
            config=ConsumerConfig.from_dict(data['config']),
            delivered=SequenceInfo.from_dict(data['delivered']),
            ack_floor=SequenceInfo.from_dict(data['ack_floor']),
            num_ack_pending=data['num_ack_pending'],
            num_redelivered=data['num_redelivered'],
            num_waiting=data['num_waiting'],
            num_pending=data['num_pending'],
            timestamp=datetime.fromisoformat(data['ts']),
            push_bound=data.get('push_bound', False),
            cluster=ClusterInfo.from_dict(data['cluster']) if 'cluster' in data else None
        )

@dataclass
class OrderedConsumerConfig:
    filter_subjects: List[str] = field(default_factory=list)
    deliver_policy: Optional[DeliverPolicy] = None
    opt_start_seq: Optional[int] = None
    opt_start_time: Optional[datetime] = None
    replay_policy: Optional[ReplayPolicy] = None
    inactive_threshold: int = 5_000_000_000  # 5 seconds in nanoseconds
    headers_only: bool = False
    max_reset_attempts: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        def convert(value):
            if isinstance(value, Enum):
                return value.name
            elif isinstance(value, datetime):
                return value.isoformat()
            return value

        result = {
            "filter_subjects": self.filter_subjects,
            "deliver_policy": self.deliver_policy.value if self.deliver_policy else None,
            "replay_policy": self.replay_policy.value if self.replay_policy else None,
            "headers_only": self.headers_only,
            "inactive_threshold": self.inactive_threshold,
        }

        if self.opt_start_seq is not None:
            result["opt_start_seq"] = self.opt_start_seq
        if self.opt_start_time is not None:
            result["opt_start_time"] = self.opt_start_time
        if self.max_reset_attempts is not None:
            result["max_reset_attempts"] = self.max_reset_attempts

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> OrderedConsumerConfig:
        kwargs = data.copy()
        return cls(**kwargs)

class ConsumerNotFoundError(Exception):
    pass

class ConsumerNameRequiredError(ValueError):
    pass

class InvalidConsumerNameError(ValueError):
    pass

class ConsumerExistsError(Exception):
    pass

class ConsumerMultipleFilterSubjectsNotSupportedError(Exception):
    pass

class Consumer(Protocol):
    @property
    def cached_info(self) -> ConsumerInfo:
        ...

class MessageBatch:
    pass

class PullConsumer(Consumer):
    def __init__(self, client: Client, stream: str, name: str, info: ConsumerInfo):
        self._client = client
        self._stream = stream
        self._name = name
        self._cached_info = info

    @property
    def cached_info(self) -> ConsumerInfo:
        return self._cached_info


    def fetch_bytes(self, max_bytes: int) -> MessageBatch:
        return MessageBatch()


    def _fetch(self) -> MessageBatch:
        return MessageBatch()

class ConsumerInfoLister(AsyncIterable):
    def __init__(self, client: Client) -> None:
        self._client = client

    def __aiter__(self) -> AsyncIterator[ConsumerInfo]:
        raise NotImplementedError

class ConsumerNameLister(AsyncIterable):
    def __aiter__(self) -> AsyncIterator[str]:
        raise NotImplementedError

class StreamConsumerManager(Protocol):
    async def create_consumer(self, stream: str, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
        """
        Creates a consumer on a given stream with given config. If consumer already exists
        and the provided configuration differs from its configuration, ErrConsumerExists is raised.
        If the provided configuration is the same as the existing consumer, the existing consumer
        is returned. Consumer interface is returned, allowing to operate on a consumer (e.g. fetch messages).
        """
        ...

    async def update_consumer(self, stream: str, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
        """
        Updates an existing consumer. If consumer does not exist, ErrConsumerDoesNotExist is raised.
        Consumer interface is returned, allowing to operate on a consumer (e.g. fetch messages).
        """
        ...

    async def create_or_update_consumer(self, stream: str, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
        """
        Creates a consumer on a given stream with given config. If consumer already exists,
        it will be updated (if possible). Consumer interface is returned, allowing to operate
        on a consumer (e.g. fetch messages).
        """
        ...

    async def consumer(self, stream: str, consumer: str, timeout: Optional[float] = None) -> Consumer:
        """
        Returns an interface to an existing consumer, allowing processing of messages.
        If consumer does not exist, ErrConsumerNotFound is raised.
        """
        ...

    async def delete_consumer(self, stream: str, consumer: str, timeout:  Optional[float] = None) -> None:
        """
        Removes a consumer with given name from a stream.
        If consumer does not exist, `ConsumerNotFoundError` is raised.
        """
        ...

class ConsumerManager(Protocol):
    async def create_consumer(self, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
        """
        Creates a consumer on a given stream with given config. If consumer already exists
        and the provided configuration differs from its configuration, `ConsumerExists` is raised.
        If the provided configuration is the same as the existing consumer, the existing consumer
        is returned.
        """
        ...

    async def update_consumer(self, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
        """
        Updates an existing consumer. If consumer does not exist, `ConsumerNotFound` is raised.
        Consumer interface is returned, allowing to operate on a consumer (e.g. fetch messages).
        """
        ...

    async def create_or_update_consumer(self, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
        """
        Creates a consumer on a given stream with given config. If consumer already exists,
        it will be updated (if possible).
        """
        ...

    async def consumer(self, consumer: str, timeout: Optional[float] = None) -> Consumer:
        """
        Returns an interface to an existing consumer, allowing processing of messages.

        If the consumer does not exist, `ConsumerNotFoundError` is raised.
        """
        ...

    async def delete_consumer(self, consumer: str, timeout: Optional[float] = None) -> None:
        """
        Removes a consumer with given name from a stream.

        If the consumer does not exist, `ConsumerNotFoundError` is raised.
        """
        ...

    def list_consumers(self) -> ConsumerInfoLister:
        """
        Returns ConsumerInfoLister enabling iterating over a channel of consumer infos.
        """
        ...

    def consumer_names(self) -> ConsumerNameLister:
        """
        Returns a ConsumerNameLister enabling iterating over a channel of consumer names.
        """
        ...


def _generate_consumer_name() -> str:
    name = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    sha = hashlib.sha256(name.encode()).digest()
    return ''.join(string.ascii_lowercase[b % 26] for b in sha[:8])

async def _upsert_consumer(client: Client, stream: str, config: ConsumerConfig, action: str, timeout: Optional[float] = None) -> Consumer:
    consumer_name = config.name
    if not consumer_name:
        if config.durable:
            consumer_name = config.durable
        else:
            consumer_name = _generate_consumer_name()

    _validate_consumer_name(consumer_name)

    if config.filter_subject and not config.filter_subjects:
        create_consumer_subject = f"CONSUMER.CREATE.{stream}.{consumer_name}.{config.filter_subject}"
    else:
        create_consumer_subject = f"CONSUMER.CREATE.{stream}.{consumer_name}"

    create_consumer_request = {
        'stream_name': stream,
        'config': config.to_dict(),
        'action': action
    }

    create_consumer_response = await client.request_json(create_consumer_subject, create_consumer_request, timeout=timeout)

    info = ConsumerInfo.from_dict(create_consumer_response)
    if config.filter_subjects and not info.config.filter_subjects:
        raise ConsumerMultipleFilterSubjectsNotSupportedError()

    # TODO support more than just pull consumers
    return PullConsumer(
        client=client,
        name=consumer_name,
        stream=stream,
        info=info,
    )

async def _create_consumer(client: Client, stream: str, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
    return await _upsert_consumer(client, stream=stream, config=config, action=CONSUMER_CREATE_ACTION, timeout=timeout)

async def _update_consumer(client: Client, stream: str, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
    return await _upsert_consumer(client, stream=stream, config=config, action=CONSUMER_UPDATE_ACTION, timeout=timeout)

async def _create_or_update_consumer(client: Client, stream: str, config: ConsumerConfig, timeout: Optional[float] = None) -> Consumer:
    return await _upsert_consumer(client, stream=stream, config=config, action=CONSUMER_CREATE_OR_UPDATE_ACTION, timeout=timeout)


async def _get_consumer(client: Client, stream: str, name: str, timeout: Optional[float] = None) -> 'Consumer':
    _validate_consumer_name(name)
    consumer_info_request = {}
    consumer_info_subject = f"CONSUMER.INFO.{stream}.{name}"

    try:
        consumer_info_response = await client.request_json(consumer_info_subject, consumer_info_request, timeout=timeout)
    except JetStreamError as jetstream_error:
        if jetstream_error.code == CONSUMER_NOT_FOUND:
            raise ConsumerNotFoundError from jetstream_error

        raise jetstream_error

    info = ConsumerInfo.from_dict(consumer_info_response)

    return PullConsumer(
        client=client,
        stream=stream,
        name=name,
        info=info,
    )

async def _delete_consumer(client: Client, stream: str, consumer: str, timeout: Optional[float] = None) -> None:
    _validate_consumer_name(consumer)

    delete_consumer_request = {}
    delete_consumer_subject = f"CONSUMER.DELETE.{stream}.{consumer}"

    try:
        delete_response = await client.request_json(delete_consumer_subject, delete_consumer_request, timeout=timeout)
    except JetStreamError as jetstream_error:
        if jetstream_error.code == CONSUMER_NOT_FOUND:
            raise ConsumerNotFoundError()

        raise jetstream_error

def _validate_consumer_name(name: str) -> None:
    if not name:
        raise ConsumerNameRequiredError()

    if any(c in name for c in ">*. /\\"):
        raise InvalidConsumerNameError()
