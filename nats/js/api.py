# Copyright 2021 The NATS Authors
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

from dataclasses import dataclass, fields, replace
from enum import Enum
from typing import Any, Dict, Optional, TypeVar, List

_NANOSECOND = 10**9


class Header(str, Enum):
    CONSUMER_STALLED = "Nats-Consumer-Stalled"
    DESCRIPTION = "Description"
    EXPECTED_LAST_MSG_ID = "Nats-Expected-Last-Msg-Id"
    EXPECTED_LAST_SEQUENCE = "Nats-Expected-Last-Sequence"
    EXPECTED_LAST_SUBJECT_SEQUENCE = "Nats-Expected-Last-Subject-Sequence"
    EXPECTED_STREAM = "Nats-Expected-Stream"
    LAST_CONSUMER = "Nats-Last-Consumer"
    LAST_STREAM = "Nats-Last-Stream"
    MSG_ID = "Nats-Msg-Id"
    ROLLUP = "Nats-Rollup"
    STATUS = "Status"


DEFAULT_PREFIX = "$JS.API"
INBOX_PREFIX = b'_INBOX.'


class StatusCode(str, Enum):
    SERVICE_UNAVAILABLE = "503"
    NO_MESSAGES = "404"
    REQUEST_TIMEOUT = "408"
    CONFLICT = "409"
    CONTROL_MESSAGE = "100"


_B = TypeVar("_B", bound="Base")


@dataclass
class Base:
    """
    Helper dataclass to filter unknown fields from the API.
    """

    @staticmethod
    def _convert(resp: Dict[str, Any], field: str, type: type[Base]) -> None:
        """Convert the field into the given type in place.
        """
        data = resp.get(field, None)
        if data is None:
            resp[field] = None
        elif isinstance(data, list):
            resp[field] = [type.from_response(item) for item in data]
        else:
            resp[field] = type.from_response(data)

    @staticmethod
    def _convert_nanoseconds(resp: Dict[str, Any], field: str) -> None:
        """Convert the given field from nanoseconds to seconds in place.
        """
        val = resp.get(field, None)
        if val is not None:
            val = val / _NANOSECOND
        resp[field] = val

    @staticmethod
    def _to_nanoseconds(val: Optional[float]) -> Optional[int]:
        """Convert the value from seconds to nanoseconds.
        """
        if val is None:
            # We use 0 to avoid sending null to Go servers.
            return 0
        return int(val * _NANOSECOND)

    @classmethod
    def from_response(cls: type[_B], resp: Dict[str, Any]) -> _B:
        """Read the class instance from a server response.

        Unknown fields are ignored ("open-world assumption").
        """
        params = {}
        for field in fields(cls):
            if field.name in resp:
                params[field.name] = resp[field.name]
        return cls(**params)

    def evolve(self: _B, **params) -> _B:
        """Return a copy of the instance with the passed values replaced.
        """
        return replace(self, **params)

    def as_dict(self) -> Dict[str, object]:
        """Return the object converted into an API-friendly dict.
        """
        result = {}
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            if isinstance(val, Base):
                val = val.as_dict()
            if isinstance(val, list):
                if len(val) > 0 and isinstance(val[0], Base):
                    val = [v.as_dict() for v in val if isinstance(v, Base)]
            result[field.name] = val
        return result


@dataclass
class PubAck(Base):
    """
    PubAck is the response of publishing a message to JetStream.
    """
    stream: str
    seq: int
    domain: Optional[str] = None
    duplicate: Optional[bool] = None


@dataclass
class Placement(Base):
    """Placement directives to consider when placing replicas of this stream"""

    cluster: Optional[str] = None
    tags: Optional[List[str]] = None


@dataclass
class ExternalStream(Base):
    api: str
    deliver: Optional[str] = None

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        return result


@dataclass
class StreamSource(Base):
    name: str
    opt_start_seq: Optional[int] = None
    # FIXME: Handle time type, omit for now.
    # opt_start_time: Optional[str] = None
    filter_subject: Optional[str] = None
    external: Optional[ExternalStream] = None
    subject_transforms: Optional[List[SubjectTransform]] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'external', ExternalStream)
        cls._convert(resp, 'subject_transforms', SubjectTransform)
        return super().from_response(resp)

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        if self.subject_transforms:
            result['subject_transform'] = [
                tr.as_dict() for tr in self.subject_transforms
            ]
        return result


@dataclass
class StreamSourceInfo(Base):
    name: str
    lag: Optional[int] = None
    active: Optional[int] = None
    error: Optional[Dict[str, Any]] = None


@dataclass
class LostStreamData(Base):
    msgs: Optional[List[int]] = None
    bytes: Optional[int] = None


@dataclass
class StreamState(Base):
    messages: int
    bytes: int
    first_seq: int
    last_seq: int
    consumer_count: int
    deleted: Optional[List[int]] = None
    num_deleted: Optional[int] = None
    lost: Optional[LostStreamData] = None
    subjects: Optional[Dict[str, int]] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'lost', LostStreamData)
        return super().from_response(resp)


class RetentionPolicy(str, Enum):
    """How message retention is considered"""

    LIMITS = "limits"
    INTEREST = "interest"
    WORK_QUEUE = "workqueue"


class StorageType(str, Enum):
    """The type of storage backend"""

    FILE = "file"
    MEMORY = "memory"


class DiscardPolicy(str, Enum):
    """Discard policy when a stream reaches its limits"""

    OLD = "old"
    NEW = "new"


class StoreCompression(str, Enum):
    """
    If stream is file-based and a compression algorithm is specified,
    the stream data will be compressed on disk.

    Valid options are none or s2 for Snappy compression.
    Introduced in nats-server 2.10.0.
    """

    NONE = "none"
    S2 = "s2"


@dataclass
class RePublish(Base):
    """
    RePublish is for republishing messages once committed to a stream. The original
    subject cis remapped from the subject pattern to the destination pattern.
    """
    src: Optional[str] = None
    dest: Optional[str] = None
    headers_only: Optional[bool] = None


@dataclass
class SubjectTransform(Base):
    """Subject transform to apply to matching messages."""
    src: str
    dest: str

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        return result


@dataclass
class StreamConfig(Base):
    """
    StreamConfig represents the configuration of a stream.
    """
    name: Optional[str] = None
    description: Optional[str] = None
    subjects: Optional[List[str]] = None
    retention: Optional[RetentionPolicy] = None
    max_consumers: Optional[int] = None
    max_msgs: Optional[int] = None
    max_bytes: Optional[int] = None
    discard: Optional[DiscardPolicy] = DiscardPolicy.OLD
    max_age: Optional[float] = None  # in seconds
    max_msgs_per_subject: int = -1
    max_msg_size: Optional[int] = -1
    storage: Optional[StorageType] = None
    num_replicas: Optional[int] = None
    no_ack: bool = False
    template_owner: Optional[str] = None
    duplicate_window: float = 0
    placement: Optional[Placement] = None
    mirror: Optional[StreamSource] = None
    sources: Optional[List[StreamSource]] = None
    sealed: bool = False
    deny_delete: bool = False
    deny_purge: bool = False
    allow_rollup_hdrs: bool = False

    # Allow republish of the message after being sequenced and stored.
    republish: Optional[RePublish] = None
    subject_transform: Optional[SubjectTransform] = None

    # Allow higher performance, direct access to get individual messages. E.g. KeyValue
    allow_direct: Optional[bool] = None

    # Allow higher performance and unified direct access for mirrors as well.
    mirror_direct: Optional[bool] = None

    # Allow compressing messages.
    compression: Optional[StoreCompression] = None

    # Metadata are user defined string key/value pairs.
    metadata: Optional[Dict[str, str]] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert_nanoseconds(resp, 'max_age')
        cls._convert_nanoseconds(resp, 'duplicate_window')
        cls._convert(resp, 'placement', Placement)
        cls._convert(resp, 'mirror', StreamSource)
        cls._convert(resp, 'sources', StreamSource)
        cls._convert(resp, 'republish', RePublish)
        cls._convert(resp, 'subject_transform', SubjectTransform)
        return super().from_response(resp)

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        result['duplicate_window'] = self._to_nanoseconds(
            self.duplicate_window
        )
        result['max_age'] = self._to_nanoseconds(self.max_age)
        if self.sources:
            result['sources'] = [src.as_dict() for src in self.sources]
        if self.compression and (self.compression != StoreCompression.NONE and self.compression != StoreCompression.S2):
            raise ValueError(
                "nats: invalid store compression type: %s" % self.compression
            )
        if self.metadata and not isinstance(self.metadata, dict):
            raise ValueError("nats: invalid metadata format")
        return result


@dataclass
class PeerInfo(Base):
    name: Optional[str] = None
    current: Optional[bool] = None
    offline: Optional[bool] = None
    active: Optional[int] = None
    lag: Optional[int] = None


@dataclass
class ClusterInfo(Base):
    leader: Optional[str] = None
    name: Optional[str] = None
    replicas: Optional[List[PeerInfo]] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'replicas', PeerInfo)
        return super().from_response(resp)


@dataclass
class StreamInfo(Base):
    """
    StreamInfo is the latest information about a stream from JetStream.
    """
    config: StreamConfig
    state: StreamState
    mirror: Optional[StreamSourceInfo] = None
    sources: Optional[List[StreamSourceInfo]] = None
    cluster: Optional[ClusterInfo] = None
    did_create: Optional[bool] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'config', StreamConfig)
        cls._convert(resp, 'state', StreamState)
        cls._convert(resp, 'mirror', StreamSourceInfo)
        cls._convert(resp, 'sources', StreamSourceInfo)
        cls._convert(resp, 'cluster', ClusterInfo)
        return super().from_response(resp)


class AckPolicy(str, Enum):
    """Policies defining how messages should be acknowledged.

    If an ack is required but is not received within the AckWait window, the message will be redelivered.

    References:
        * `Consumers, AckPolicy <https://docs.nats.io/jetstream/concepts/consumers#ackpolicy>`_
    """

    NONE = "none"
    ALL = "all"
    EXPLICIT = "explicit"


class DeliverPolicy(str, Enum):
    """When a consumer is first created, it can specify where in the stream it wants to start receiving messages.

    This is the DeliverPolicy, and this enumeration defines allowed values.

    References:
        * `Consumers, DeliverPolicy/OptStartSeq/OptStartTime <https://docs.nats.io/jetstream/concepts/consumers#deliverpolicy-optstartseq-optstarttime>`_
    """  # noqa: E501

    ALL = "all"
    LAST = "last"
    NEW = "new"
    BY_START_SEQUENCE = "by_start_sequence"
    BY_START_TIME = "by_start_time"
    LAST_PER_SUBJECT = "last_per_subject"


class ReplayPolicy(str, Enum):
    """The replay policy applies when the DeliverPolicy is one of:
        * all
        * by_start_sequence
        * by_start_time
    since those deliver policies begin reading the stream at a position other than the end.

    References:
        * `Consumers, ReplayPolicy <https://docs.nats.io/jetstream/concepts/consumers#replaypolicy>`_
    """

    INSTANT = "instant"
    ORIGINAL = "original"


@dataclass
class ConsumerConfig(Base):
    """Consumer configuration.

    References:
        * `Consumers <https://docs.nats.io/jetstream/concepts/consumers>`_
    """
    name: Optional[str] = None
    durable_name: Optional[str] = None
    description: Optional[str] = None
    deliver_policy: Optional[DeliverPolicy] = DeliverPolicy.ALL
    opt_start_seq: Optional[int] = None
    opt_start_time: Optional[int] = None
    ack_policy: Optional[AckPolicy] = AckPolicy.EXPLICIT
    ack_wait: Optional[float] = None  # in seconds
    max_deliver: Optional[int] = None
    backoff: Optional[List[float]] = None  # in seconds, overrides ack_wait
    filter_subject: Optional[str] = None
    filter_subjects: Optional[List[str]] = None
    replay_policy: Optional[ReplayPolicy] = ReplayPolicy.INSTANT
    rate_limit_bps: Optional[int] = None
    sample_freq: Optional[str] = None
    max_waiting: Optional[int] = None
    max_ack_pending: Optional[int] = None
    flow_control: Optional[bool] = None
    idle_heartbeat: Optional[float] = None
    headers_only: Optional[bool] = None

    # Push based consumers.
    deliver_subject: Optional[str] = None
    # Push based queue consumers.
    deliver_group: Optional[str] = None

    # Ephemeral inactivity threshold
    inactive_threshold: Optional[float] = None  # in seconds

    # Generally inherited by parent stream and other markers, now can
    # be configured directly.
    num_replicas: Optional[int] = None

    # Force memory storage.
    mem_storage: Optional[bool] = None

    # Metadata are user defined string key/value pairs.
    metadata: Optional[Dict[str, str]] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert_nanoseconds(resp, 'ack_wait')
        cls._convert_nanoseconds(resp, 'idle_heartbeat')
        cls._convert_nanoseconds(resp, 'inactive_threshold')
        if 'backoff' in resp:
            resp['backoff'] = [val / _NANOSECOND for val in resp['backoff']]
        return super().from_response(resp)

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        result['ack_wait'] = self._to_nanoseconds(self.ack_wait)
        result['idle_heartbeat'] = self._to_nanoseconds(self.idle_heartbeat)
        result['inactive_threshold'] = self._to_nanoseconds(
            self.inactive_threshold
        )
        if self.backoff:
            result['backoff'] = [self._to_nanoseconds(i) for i in self.backoff]
        return result


@dataclass
class SequenceInfo(Base):
    consumer_seq: int
    stream_seq: int
    # FIXME: Do not handle dates for now.
    # last_active: Optional[datetime]


@dataclass
class ConsumerInfo(Base):
    """
    ConsumerInfo represents the info about the consumer.
    """
    name: str
    stream_name: str
    config: ConsumerConfig
    # FIXME: Do not handle dates for now.
    # created: datetime
    delivered: Optional[SequenceInfo] = None
    ack_floor: Optional[SequenceInfo] = None
    num_ack_pending: Optional[int] = None
    num_redelivered: Optional[int] = None
    num_waiting: Optional[int] = None
    num_pending: Optional[int] = None
    cluster: Optional[ClusterInfo] = None
    push_bound: Optional[bool] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'delivered', SequenceInfo)
        cls._convert(resp, 'ack_floor', SequenceInfo)
        cls._convert(resp, 'config', ConsumerConfig)
        cls._convert(resp, 'cluster', ClusterInfo)
        return super().from_response(resp)


@dataclass
class AccountLimits(Base):
    """Account limits

    References:
        * `Multi-tenancy & Resource Mgmt <https://docs.nats.io/jetstream/resource_management>`_
    """

    max_memory: int
    max_storage: int
    max_streams: int
    max_consumers: int
    max_ack_pending: int
    memory_max_stream_bytes: int
    storage_max_stream_bytes: int
    max_bytes_required: bool


@dataclass
class Tier(Base):
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: AccountLimits

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'limits', AccountLimits)
        return super().from_response(resp)


@dataclass
class APIStats(Base):
    """API stats"""

    total: int
    errors: int


@dataclass
class AccountInfo(Base):
    """Account information

    References:
        * `Account Information <https://docs.nats.io/jetstream/administration/account#account-information>`_
    """
    # NOTE: These fields are shared with Tier type as well.
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: AccountLimits

    api: APIStats
    domain: Optional[str] = None
    tiers: Optional[Dict[str, Tier]] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'limits', AccountLimits)
        cls._convert(resp, 'api', APIStats)
        info = super().from_response(resp)
        tiers = resp.get('tiers', None)
        if tiers:
            result = {}
            for k, v in tiers.items():
                result[k] = Tier.from_response(v)
            info.tiers = result
        return info


@dataclass
class RawStreamMsg(Base):
    subject: Optional[str] = None
    seq: Optional[int] = None
    data: Optional[bytes] = None
    hdrs: Optional[bytes] = None
    headers: Optional[Dict] = None
    stream: Optional[str] = None
    # TODO: Add 'time'

    @property
    def sequence(self) -> Optional[int]:
        return self.seq

    @property
    def header(self) -> Optional[Dict]:
        """
        header returns the headers from a message.
        """
        return self.headers


@dataclass
class KeyValueConfig(Base):
    """
    KeyValueConfig is the configuration of a KeyValue store.
    """
    bucket: str
    description: Optional[str] = None
    max_value_size: Optional[int] = None
    history: int = 1
    ttl: Optional[float] = None  # in seconds
    max_bytes: Optional[int] = None
    storage: Optional[StorageType] = None
    replicas: int = 1
    placement: Optional[Placement] = None
    republish: Optional[RePublish] = None
    direct: Optional[bool] = None

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        result['ttl'] = self._to_nanoseconds(self.ttl)
        return result


@dataclass
class StreamPurgeRequest(Base):
    """
    StreamPurgeRequest is optional request information to the purge API.
    """
    # Purge up to but not including sequence.
    seq: Optional[int] = None
    # Subject to match against messages for the purge command.
    filter: Optional[str] = None
    # Number of messages to keep.
    keep: Optional[int] = None


@dataclass
class ObjectStoreConfig(Base):
    """
    ObjectStoreConfig is the configurigation of an ObjectStore.
    """
    bucket: Optional[str] = None
    description: Optional[str] = None
    ttl: Optional[float] = None
    max_bytes: Optional[int] = None
    storage: Optional[StorageType] = None
    replicas: int = 1
    placement: Optional[Placement] = None

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        result['ttl'] = self._to_nanoseconds(self.ttl)
        return result


@dataclass
class ObjectLink(Base):
    """
    ObjectLink is used to embed links to other buckets and objects.
    """
    # Bucket is the name of the other object store.
    bucket: str
    #  Name can be used to link to a single object.
    # If empty means this is a link to the whole store, like a directory.
    name: Optional[str] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        return super().from_response(resp)


@dataclass
class ObjectMetaOptions(Base):
    link: Optional[ObjectLink] = None
    max_chunk_size: Optional[int] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'link', ObjectLink)
        return super().from_response(resp)


@dataclass
class ObjectMeta(Base):
    """
    ObjectMeta is high level information about an object.
    """
    name: Optional[str] = None
    description: Optional[str] = None
    headers: Optional[dict] = None
    #  Optional options.
    options: Optional[ObjectMetaOptions] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'options', ObjectMetaOptions)
        return super().from_response(resp)


@dataclass
class ObjectInfo(Base):
    """
    ObjectInfo is meta plus instance information.
    """
    name: str
    bucket: str
    nuid: str
    size: Optional[int] = None
    mtime: Optional[str] = None
    chunks: Optional[int] = None
    digest: Optional[str] = None
    deleted: Optional[bool] = False
    description: Optional[str] = None
    headers: Optional[dict] = None
    #  Optional options.
    options: Optional[ObjectMetaOptions] = None
    # NOTE: name, description, headers, options together compose
    # what would be the ObjectMeta embedded type in Go.

    @property
    def meta(self) -> ObjectMeta:
        return ObjectMeta(
            name=self.name,
            description=self.description,
            headers=self.headers,
            options=self.options,
        )

    def is_link(self) -> bool:
        return self.options is not None and self.options.link is not None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'options', ObjectMetaOptions)
        return super().from_response(resp)
