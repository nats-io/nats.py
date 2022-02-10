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

from dataclasses import dataclass, fields, replace
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar

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
    CONTROL_MESSAGE = "100"


_B = TypeVar("_B", bound="Base")


@dataclass
class Base:
    """
    Helper dataclass to filter unknown fields from the API.
    """

    @staticmethod
    def _convert(resp: Dict[str, Any], field: str, type: Type["Base"]) -> None:
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
            return None
        return int(val * _NANOSECOND)

    @classmethod
    def from_response(cls: Type[_B], resp: Dict[str, Any]) -> _B:
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

    cluster: str
    tags: Optional[List[str]] = None


@dataclass
class ExternalStream(Base):
    api: str
    deliver: Optional[str] = None


@dataclass
class StreamSource(Base):
    name: str
    opt_start_seq: Optional[int] = None
    # FIXME: Handle time type, omit for now.
    # opt_start_time: Optional[str] = None
    filter_subject: Optional[str] = None
    external: Optional[ExternalStream] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'external', ExternalStream)
        return super().from_response(resp)


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
    duplicate_window: int = 0
    placement: Optional[Placement] = None
    mirror: Optional[StreamSource] = None
    sources: Optional[List[StreamSource]] = None
    sealed: bool = False
    deny_delete: bool = False
    deny_purge: bool = False
    allow_rollup_hdrs: bool = False

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert_nanoseconds(resp, 'max_age')
        cls._convert(resp, 'placement', Placement)
        cls._convert(resp, 'mirror', StreamSource)
        cls._convert(resp, 'sources', StreamSource)
        return super().from_response(resp)

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        result['max_age'] = self._to_nanoseconds(self.max_age)
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
    LAST_PER_SUBJECT = "last_per_subject"
    BY_START_SEQUENCE = "by_start_sequence"
    BY_START_TIME = "by_start_time"


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
    durable_name: Optional[str] = None
    description: Optional[str] = None
    deliver_subject: Optional[str] = None
    deliver_group: Optional[str] = None
    deliver_policy: Optional[DeliverPolicy] = DeliverPolicy.ALL
    opt_start_seq: Optional[int] = None
    opt_start_time: Optional[int] = None
    ack_policy: Optional[AckPolicy] = AckPolicy.EXPLICIT
    ack_wait: Optional[float] = None  # in seconds
    max_deliver: Optional[int] = None
    filter_subject: Optional[str] = None
    replay_policy: Optional[ReplayPolicy] = ReplayPolicy.INSTANT
    sample_freq: Optional[str] = None
    rate_limit_bps: Optional[int] = None
    max_waiting: Optional[int] = None
    max_ack_pending: Optional[int] = None
    flow_control: Optional[bool] = None
    idle_heartbeat: Optional[float] = None
    headers_only: Optional[bool] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert_nanoseconds(resp, 'ack_wait')
        cls._convert_nanoseconds(resp, 'idle_heartbeat')
        return super().from_response(resp)

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        result['ack_wait'] = self._to_nanoseconds(self.ack_wait)
        result['idle_heartbeat'] = self._to_nanoseconds(self.idle_heartbeat)
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

    memory: int
    storage: int
    streams: int
    consumers: int
    limits: AccountLimits
    api: APIStats
    domain: Optional[str] = None

    @classmethod
    def from_response(cls, resp: Dict[str, Any]):
        cls._convert(resp, 'limits', AccountLimits)
        cls._convert(resp, 'api', APIStats)
        return super().from_response(resp)


@dataclass
class RawStreamMsg(Base):
    subject: Optional[str] = None
    seq: Optional[int] = None
    data: Optional[bytes] = None
    hdrs: Optional[bytes] = None
    headers: Optional[dict] = None
    # TODO: Add 'time'

    @property
    def sequence(self) -> Optional[int]:
        return self.seq


@dataclass
class KeyValueConfig(Base):
    """
    KeyValueConfig is the configurigation of a KeyValue store.
    """
    bucket: str
    description: Optional[str] = None
    max_value_size: Optional[int] = None
    history: int = 1
    ttl: Optional[float] = None  # in seconds
    max_bytes: Optional[int] = None
    storage: Optional[StorageType] = None
    replicas: int = 1

    def as_dict(self) -> Dict[str, object]:
        result = super().as_dict()
        result['ttl'] = self._to_nanoseconds(self.ttl)
        return result
