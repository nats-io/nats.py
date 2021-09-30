from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional

from .base import JetStreamResponse, parse_datetime
from .clusters import Cluster


class AckPolicy(str, Enum):
    """Policies defining how messages should be adcknowledged.

    If an ack is required but is not received within the AckWait window, the message will be redelivered.

    References:
        * Consumers, AckPolicy - [NATS Docs](https://docs.nats.io/jetstream/concepts/consumers#ackpolicy)
    """

    none = "none"
    all = "all"
    explicit = "explicit"


class DeliverPolicy(str, Enum):
    """When a consumer is first created, it can specify where in the stream it wants to start receiving messages.

    This is the DeliverPolicy, and this enumeration defines allowed values.

    References:
        * Consumers, DeliverPolicy/OptStartSeq/OptStartTime - [NATS Docs](https://docs.nats.io/jetstream/concepts/consumers#deliverpolicy-optstartseq-optstarttime)
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
        * Consumers, ReplayPolicy - [NATS Docs](https://docs.nats.io/jetstream/concepts/consumers#replaypolicy)
    """

    instant = "instant"
    original = "original"


@dataclass
class ConsumerConfig:
    """Consumer configuration.

    Field descriptions are available in source code.

    References:
        * Consumers - [NATS Docs](https://docs.nats.io/jetstream/concepts/consumers)
    """

    deliver_policy: DeliverPolicy = DeliverPolicy.last
    deliver_group: Optional[str] = None
    ack_policy: AckPolicy = AckPolicy.explicit
    durable_name: Optional[str] = None
    deliver_subject: Optional[str] = None
    ack_wait: Optional[int] = None
    max_deliver: Optional[int] = None
    filter_subject: Optional[str] = None
    replay_policy: ReplayPolicy = ReplayPolicy.instant
    sample_freq: Optional[str] = None
    rate_limit_bps: Optional[int] = None
    max_ack_pending: Optional[int] = None
    idle_heartbeat: Optional[int] = None
    flow_control: Optional[bool] = None
    max_waiting: Optional[int] = 512
    ops_start_seq: Optional[int] = None
    ops_start_time: Optional[int] = None


@dataclass
class Delivered:
    """Last message delivered from this consumer

    Fields descriptions are available in source code.
    """

    consumer_seq: int
    stream_seq: int
    last: Optional[datetime]

    def __post_init__(self):
        if isinstance(self.last, str):
            self.last = parse_datetime(self.last)


@dataclass
class AckFloor:
    """Highest contiguous acknowledged message

    Fields descriptions are available in source code.
    """

    consumer_seq: int
    stream_seq: int
    last: Optional[datetime]

    def __post_init__(self):
        if isinstance(self.last, str):
            self.last = parse_datetime(self.last)


@dataclass
class Consumer:
    """View of a consumer"""

    stream_name: str
    config: ConsumerConfig
    created: datetime
    delivered: Delivered
    ack_floor: AckFloor
    num_ack_pending: int
    num_redelivered: int
    num_waiting: int
    num_pending: int
    name: Optional[str] = None
    cluster: Optional[Cluster] = None
    push_bound: Optional[bool] = None

    def __post_init__(self):
        if isinstance(self.created, str):
            self.created = parse_datetime(self.created)
        if isinstance(self.config, dict):
            self.config = ConsumerConfig(**self.config)
        if isinstance(self.cluster, dict):
            self.cluster = Cluster(**self.cluster)


@dataclass
class ConsumerCreateResponse(Consumer, JetStreamResponse):
    """Reply from `$JS.API.CONSUMER.CREATE.*.*`"""

    pass


@dataclass
class ConsumerInfoResponse(Consumer, JetStreamResponse):
    """Reply from `$JS.API.CONSUMER.INFO.*.*`"""

    pass


@dataclass
class ConsumerDeleteResponse(JetStreamResponse):
    """Reply from `$JS.API.CONSUMER.DELETE.*.*`"""

    success: bool


@dataclass
class ConsumerListResponse(JetStreamResponse):
    """Reply from `$JS.API.CONSUMER.LIST.*`"""

    total: int
    offset: int
    limit: int
    consumers: List[Consumer] = field(default_factory=list)

    def __post_init__(self):
        if self.consumers is None:
            self.consumers = []
        self.consumers = [
            Consumer(**item) if isinstance(item, dict) else item
            for item in self.consumers
        ]


@dataclass
class ConsumerNamesResponse(JetStreamResponse):
    """Reply from `$JS.API.CONSUMER.NAMES.*`"""

    total: int
    offset: int
    limit: int
    consumers: List[str] = field(default_factory=list)


@dataclass
class ConsumerCreateRequest:
    """Request options for `$JS.API.CONSUMER.CREATE.*.*`"""

    stream_name: str
    config: ConsumerConfig

    def __post_init__(self):
        if isinstance(self.config, dict):
            self.config = ConsumerConfig(**self.config)


@dataclass
class ConsumerListRequest:
    """Request options for `$JS.API.CONSUMER.LIST.*`"""

    offset: Optional[int] = None


@dataclass
class ConsumerNamesRequest:
    """Request options for `$JS.API.CONSUMER.NAMES.*`"""

    offset: Optional[int] = None
    subject: Optional[str] = None


@dataclass
class ConsumerGetNextRequest:
    """Request options for `$JS.API.CONSUMER.MSG.NEXT.*.*`"""

    expires: Optional[int] = 5000000000
    batch: Optional[int] = 1
    no_wait: Optional[bool] = False
