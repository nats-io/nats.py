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

from dataclasses import asdict, dataclass, fields
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar
import json


class Header(str, Enum):
    MSG_ID = "Nats-Msg-Id"
    EXPECTED_STREAM = "Nats-Expected-Stream"
    EXPECTED_LAST_SEQUENCE = "Nats-Expected-Last-Sequence"
    EXPECTED_LAST_SUBJECT_SEQUENCE = "Nats-Expected-Last-Subject-Sequence"
    EXPECTED_LAST_MSG_ID = "Nats-Expected-Last-Msg-Id"
    ROLLUP = "Nats-Rollup"
    LAST_CONSUMER = "Nats-Last-Consumer"
    LAST_STREAM = "Nats-Last-Stream"
    STATUS = "Status"
    DESCRIPTION = "Description"


DefaultPrefix = "$JS.API"
InboxPrefix = bytearray(b'_INBOX.')


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
    @classmethod
    def properties(klass, **opts) -> List[str]:
        return [f.name for f in fields(klass)]

    @classmethod
    def loads(klass: Type[_B], **opts) -> _B:
        # Reject unknown properties before loading.
        # FIXME: Find something more efficient...
        to_rm = []
        for e in opts:
            if e not in klass.properties():
                to_rm.append(e)

        for m in to_rm:
            del opts[m]
        return klass(**opts)  # type: ignore[call-arg]

    def asjson(self) -> str:
        # Filter and remove any null values since invalid for Go.
        cfg = asdict(self)
        for k, v in dict(cfg).items():
            if v is None:
                del cfg[k]
        return json.dumps(cfg)


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

    def __post_init__(self) -> None:
        if isinstance(self.external, dict):
            self.external = ExternalStream.loads(**self.external)


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

    def __post_init__(self) -> None:
        if isinstance(self.lost, dict):
            self.lost = LostStreamData.loads(**self.lost)


class RetentionPolicy(str, Enum):
    """How message retention is considered"""

    limits = "limits"
    interest = "interest"
    workqueue = "workqueue"


class StorageType(str, Enum):
    """The type of storage backend"""

    file = "file"
    memory = "memory"


class DiscardPolicy(str, Enum):
    """Discard policy when a stream reaches its limits"""

    old = "old"
    new = "new"


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
    discard: Optional[DiscardPolicy] = DiscardPolicy.old
    max_age: Optional[int] = None
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

    def __post_init__(self) -> None:
        if isinstance(self.placement, dict):
            self.placement = Placement.loads(**self.placement)
        if isinstance(self.mirror, dict):
            self.mirror = StreamSource.loads(**self.mirror)
        if self.sources:
            self.sources = [
                StreamSource.loads(**item) if isinstance(item, dict) else item
                for item in self.sources
            ]


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

    def __post_init__(self) -> None:
        if self.replicas:
            self.replicas = [
                PeerInfo.loads(**item) if isinstance(item, dict) else item
                for item in self.replicas
            ]


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

    def __post_init__(self) -> None:
        if isinstance(self.config, dict):
            self.config = StreamConfig.loads(**self.config)
        if isinstance(self.state, dict):
            self.state = StreamState.loads(**self.state)
        if isinstance(self.mirror, dict):
            self.mirror = StreamSourceInfo.loads(**self.mirror)
        if self.sources:
            self.sources = [
                StreamSourceInfo.loads(**item)
                if isinstance(item, dict) else item for item in self.sources
            ]
        if isinstance(self.cluster, dict):
            self.cluster = ClusterInfo.loads(**self.cluster)


class AckPolicy(str, Enum):
    """Policies defining how messages should be acknowledged.

    If an ack is required but is not received within the AckWait window, the message will be redelivered.

    References:
        * `Consumers, AckPolicy <https://docs.nats.io/jetstream/concepts/consumers#ackpolicy>`_
    """

    none = "none"
    all = "all"
    explicit = "explicit"


class DeliverPolicy(str, Enum):
    """When a consumer is first created, it can specify where in the stream it wants to start receiving messages.

    This is the DeliverPolicy, and this enumeration defines allowed values.

    References:
        * `Consumers, DeliverPolicy/OptStartSeq/OptStartTime <https://docs.nats.io/jetstream/concepts/consumers#deliverpolicy-optstartseq-optstarttime>`_
    """

    all = "all"
    last = "last"
    new = "new"
    last_per_subject = "last_per_subject"
    by_start_sequence = "by_start_sequence"
    by_start_time = "by_start_time"


class ReplayPolicy(str, Enum):
    """The replay policy applies when the DeliverPolicy is one of:
        * all
        * by_start_sequence
        * by_start_time
    since those deliver policies begin reading the stream at a position other than the end.

    References:
        * `Consumers, ReplayPolicy <https://docs.nats.io/jetstream/concepts/consumers#replaypolicy>`_
    """

    instant = "instant"
    original = "original"


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
    deliver_policy: Optional[DeliverPolicy] = DeliverPolicy.all
    opt_start_seq: Optional[int] = None
    opt_start_time: Optional[int] = None
    ack_policy: Optional[AckPolicy] = AckPolicy.explicit
    # ack_wait in seconds
    ack_wait: Optional[int] = None
    max_deliver: Optional[int] = None
    filter_subject: Optional[str] = None
    replay_policy: Optional[ReplayPolicy] = ReplayPolicy.instant
    sample_freq: Optional[str] = None
    rate_limit_bps: Optional[int] = None
    max_waiting: Optional[int] = None
    max_ack_pending: Optional[int] = None
    flow_control: Optional[bool] = None
    idle_heartbeat: Optional[int] = None
    headers_only: Optional[bool] = None

    def __post_init__(self) -> None:
        if self.ack_wait:
            self.ack_wait = self.ack_wait // 1_000_000_000


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

    def __post_init__(self) -> None:
        if isinstance(self.delivered, dict):
            self.delivered = SequenceInfo.loads(**self.delivered)
        if isinstance(self.ack_floor, dict):
            self.ack_floor = SequenceInfo.loads(**self.ack_floor)
        if isinstance(self.config, dict):
            self.config = ConsumerConfig.loads(**self.config)
        if isinstance(self.cluster, dict):
            self.cluster = ClusterInfo.loads(**self.cluster)


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

    def __post_init__(self) -> None:
        if isinstance(self.limits, dict):
            self.limits = AccountLimits.loads(**self.limits)
        if isinstance(self.api, dict):
            self.api = APIStats.loads(**self.api)


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
    history: Optional[int] = None
    ttl: Optional[int] = None  # in seconds
    max_bytes: Optional[int] = None
    storage: Optional[StorageType] = None
    replicas: Optional[int] = None
