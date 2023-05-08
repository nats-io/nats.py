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
from typing import Any, Dict, Optional, TypeVar

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
    def _convert(resp: dict[str, Any], field: str, type: type[Base]) -> None:
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
    def _convert_nanoseconds(resp: dict[str, Any], field: str) -> None:
        """Convert the given field from nanoseconds to seconds in place.
        """
        val = resp.get(field, None)
        if val is not None:
            val = val / _NANOSECOND
        resp[field] = val

    @staticmethod
    def _to_nanoseconds(val: float | None) -> int | None:
        """Convert the value from seconds to nanoseconds.
        """
        if val is None:
            # We use 0 to avoid sending null to Go servers.
            return 0
        return int(val * _NANOSECOND)

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

    def evolve(self: _B, **params) -> _B:
        """Return a copy of the instance with the passed values replaced.
        """
        return replace(self, **params)

    def as_dict(self) -> dict[str, object]:
        """Return the object converted into an API-friendly dict.
        """
        result = {}
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            if isinstance(val, Base):
                val = val.as_dict()
            result[field.name] = val
        return result


@dataclass
class PubAck(Base):
    """
    PubAck is the response of publishing a message to JetStream.
    """
    stream: str
    seq: int
    domain: str | None = None
    duplicate: bool | None = None


@dataclass
class Placement(Base):
    """Placement directives to consider when placing replicas of this stream"""

    cluster: str | None = None
    tags: list[str] | None = None


@dataclass
class ExternalStream(Base):
    api: str
    deliver: str | None = None


@dataclass
class StreamSource(Base):
    name: str
    opt_start_seq: int | None = None
    # FIXME: Handle time type, omit for now.
    # opt_start_time: Optional[str] = None
    filter_subject: str | None = None
    external: ExternalStream | None = None

    @classmethod
    def from_response(cls, resp: dict[str, Any]):
        cls._convert(resp, 'external', ExternalStream)
        return super().from_response(resp)


@dataclass
class StreamSourceInfo(Base):
    name: str
    lag: int | None = None
    active: int | None = None
    error: dict[str, Any] | None = None


@dataclass
class LostStreamData(Base):
    msgs: list[int] | None = None
    bytes: int | None = None


@dataclass
class StreamState(Base):
    messages: int
    bytes: int
    first_seq: int
    last_seq: int
    consumer_count: int
    deleted: list[int] | None = None
    num_deleted: int | None = None
    lost: LostStreamData | None = None

    @classmethod
    def from_response(cls, resp: dict[str, Any]):
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


@dataclass
class RePublish(Base):
    """
    RePublish is for republishing messages once committed to a stream. The original
    subject cis remapped from the subject pattern to the destination pattern.
    """
    src: str | None = None
    dest: str | None = None
    headers_only: bool | None = None


@dataclass
class StreamConfig(Base):
    """
    StreamConfig represents the configuration of a stream.
    """
    name: str | None = None
    description: str | None = None
    subjects: list[str] | None = None
    retention: RetentionPolicy | None = None
    max_consumers: int | None = None
    max_msgs: int | None = None
    max_bytes: int | None = None
    discard: DiscardPolicy | None = DiscardPolicy.OLD
    max_age: float | None = None  # in seconds
    max_msgs_per_subject: int = -1
    max_msg_size: int | None = -1
    storage: StorageType | None = None
    num_replicas: int | None = None
    no_ack: bool = False
    template_owner: str | None = None
    duplicate_window: float = 0
    placement: Placement | None = None
    mirror: StreamSource | None = None
    sources: list[StreamSource] | None = None
    sealed: bool = False
    deny_delete: bool = False
    deny_purge: bool = False
    allow_rollup_hdrs: bool = False

    # Allow republish of the message after being sequenced and stored.
    republish: RePublish | None = None

    # Allow higher performance, direct access to get individual messages. E.g. KeyValue
    allow_direct: bool | None = None

    # Allow higher performance and unified direct access for mirrors as well.
    mirror_direct: bool | None = None

    @classmethod
    def from_response(cls, resp: dict[str, Any]):
        cls._convert_nanoseconds(resp, 'max_age')
        cls._convert_nanoseconds(resp, 'duplicate_window')
        cls._convert(resp, 'placement', Placement)
        cls._convert(resp, 'mirror', StreamSource)
        cls._convert(resp, 'sources', StreamSource)
        cls._convert(resp, 'republish', RePublish)
        return super().from_response(resp)

    def as_dict(self) -> dict[str, object]:
        result = super().as_dict()
        result['duplicate_window'] = self._to_nanoseconds(
            self.duplicate_window
        )
        result['max_age'] = self._to_nanoseconds(self.max_age)
        return result


@dataclass
class PeerInfo(Base):
    name: str | None = None
    current: bool | None = None
    offline: bool | None = None
    active: int | None = None
    lag: int | None = None


@dataclass
class ClusterInfo(Base):
    leader: str | None = None
    name: str | None = None
    replicas: list[PeerInfo] | None = None

    @classmethod
    def from_response(cls, resp: dict[str, Any]):
        cls._convert(resp, 'replicas', PeerInfo)
        return super().from_response(resp)


@dataclass
class StreamInfo(Base):
    """
    StreamInfo is the latest information about a stream from JetStream.
    """
    config: StreamConfig
    state: StreamState
    mirror: StreamSourceInfo | None = None
    sources: list[StreamSourceInfo] | None = None
    cluster: ClusterInfo | None = None
    did_create: bool | None = None

    @classmethod
    def from_response(cls, resp: dict[str, Any]):
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
    name: str | None = None
    durable_name: str | None = None
    description: str | None = None
    deliver_policy: DeliverPolicy | None = DeliverPolicy.ALL
    opt_start_seq: int | None = None
    opt_start_time: int | None = None
    ack_policy: AckPolicy | None = AckPolicy.EXPLICIT
    ack_wait: float | None = None  # in seconds
    max_deliver: int | None = None
    filter_subject: str | None = None
    replay_policy: ReplayPolicy | None = ReplayPolicy.INSTANT
    rate_limit_bps: int | None = None
    sample_freq: str | None = None
    max_waiting: int | None = None
    max_ack_pending: int | None = None
    flow_control: bool | None = None
    idle_heartbeat: float | None = None
    headers_only: bool | None = None

    # Push based consumers.
    deliver_subject: str | None = None
    deliver_group: str | None = None

    # Ephemeral inactivity threshold
    inactive_threshold: float | None = None  # in seconds

    # Generally inherited by parent stream and other markers, now can
    # be configured directly.
    num_replicas: int | None = None

    # Force memory storage.
    mem_storage: bool | None = None

    @classmethod
    def from_response(cls, resp: dict[str, Any]):
        cls._convert_nanoseconds(resp, 'ack_wait')
        cls._convert_nanoseconds(resp, 'idle_heartbeat')
        cls._convert_nanoseconds(resp, 'inactive_threshold')
        return super().from_response(resp)

    def as_dict(self) -> dict[str, object]:
        result = super().as_dict()
        result['ack_wait'] = self._to_nanoseconds(self.ack_wait)
        result['idle_heartbeat'] = self._to_nanoseconds(self.idle_heartbeat)
        result['inactive_threshold'] = self._to_nanoseconds(
            self.inactive_threshold
        )
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
    delivered: SequenceInfo | None = None
    ack_floor: SequenceInfo | None = None
    num_ack_pending: int | None = None
    num_redelivered: int | None = None
    num_waiting: int | None = None
    num_pending: int | None = None
    cluster: ClusterInfo | None = None
    push_bound: bool | None = None

    @classmethod
    def from_response(cls, resp: dict[str, Any]):
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
    def from_response(cls, resp: dict[str, Any]):
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
    domain: str | None = None
    tiers: dict[str, Tier] | None = None

    @classmethod
    def from_response(cls, resp: dict[str, Any]):
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
    subject: str | None = None
    seq: int | None = None
    data: bytes | None = None
    hdrs: bytes | None = None
    headers: dict | None = None
    stream: str | None = None
    # TODO: Add 'time'

    @property
    def sequence(self) -> int | None:
        return self.seq

    @property
    def header(self) -> dict | None:
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
    description: str | None = None
    max_value_size: int | None = None
    history: int = 1
    ttl: float | None = None  # in seconds
    max_bytes: int | None = None
    storage: StorageType | None = None
    replicas: int = 1
    placement: Placement | None = None
    republish: RePublish | None = None
    direct: bool | None = None

    def as_dict(self) -> dict[str, object]:
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
    bucket: str
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
    name: str
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
    size: int
    mtime: str
    chunks: int
    digest: Optional[str] = None
    deleted: Optional[bool] = None
    description: Optional[str] = None
    headers: Optional[dict] = None
    #  Optional options.
    options: Optional[ObjectMetaOptions] = None

    def is_link(self) -> bool:
        return self.options is not None and self.options.link is not None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'options', ObjectMetaOptions)
        return super().from_response(resp)
