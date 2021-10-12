from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from .base import JetStreamResponse
from .clusters import Cluster
from .messages import Message


class Retention(str, Enum):
    """How message retention is considered"""

    limits = "limits"
    interest = "interest"
    workqueue = "workqueue"


class Storage(str, Enum):
    """The type of storage backend"""

    file = "file"
    memory = "memory"


class Discard(str, Enum):
    """Discard policy when a stream reaches it's limits"""

    old = "old"
    new = "new"


@dataclass
class Placement:
    """Placement directives to consider when placing replicas of this stream"""

    cluster: str
    tags: Optional[List[str]] = None


@dataclass
class External:
    api: str
    deliver: Optional[str] = None


@dataclass
class Mirror:
    """Placement directives to consider when placing replicas of this stream, random placement when unset"""

    name: str
    opt_start_seq: Optional[int] = None
    opt_start_time: Optional[str] = None
    filter_subject: Optional[str] = None
    external: Optional[External] = None

    def __post_init__(self):
        if isinstance(self.external, dict):
            self.external = External(**self.external)


@dataclass
class MirrorInfo:
    name: str
    lag: Optional[int] = None
    active: Optional[int] = None
    error: Optional[Dict[str, Any]] = None


@dataclass
class Source:
    name: str
    opt_start_seq: Optional[int] = None
    opt_start_time: Optional[str] = None
    filter_subject: Optional[str] = None
    external: Optional[External] = None

    def __post_init__(self):
        if isinstance(self.external, dict):
            self.external = External(**self.external)


@dataclass
class SourceInfo:
    name: str
    lag: Optional[int] = None
    active: Optional[int] = None
    error: Optional[Dict[str, Any]] = None


@dataclass
class StreamConfig:
    """Stream configuration

    References:
        Streams - [NATS Docs](https://docs.nats.io/jetstream/concepts/streams)
    """

    retention: Retention
    max_consumers: int
    max_msgs: int
    max_bytes: int
    max_age: int
    storage: Storage
    num_replicas: int
    name: Optional[str] = None
    subjects: Optional[List[str]] = None
    max_msgs_per_subject: Optional[int] = -1
    max_msg_size: Optional[int] = -1
    no_ack: Optional[bool] = False
    template_owner: Optional[str] = None
    discard: Optional[Discard] = Discard.old
    duplicate_window: Optional[int] = 0
    placement: Optional[Placement] = None
    mirror: Optional[Mirror] = None
    sources: Optional[List[Source]] = None

    def __post_init__(self):
        if isinstance(self.placement, dict):
            self.placement = Placement(**self.placement)
        if isinstance(self.mirror, dict):
            self.mirror = Mirror(**self.mirror)
        if self.sources:
            self.sources = [
                Source(**item) if isinstance(item, dict) else item
                for item in self.sources
            ]


@dataclass
class Lost:
    msgs: Optional[List[int]] = None
    bytes: Optional[int] = None


@dataclass
class State:
    messages: int
    bytes: int
    first_seq: int
    last_seq: int
    consumer_count: int
    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None
    deleted: Optional[List[int]] = None
    num_deleted: Optional[int] = None
    lost: Optional[Lost] = None

    def __post_init__(self):
        if isinstance(self.lost, dict):
            self.lost = Lost(**self.lost)


@dataclass
class PubAck:
    stream: str
    seq: int
    domain: Optional[str] = None
    duplicate: Optional[bool] = None


@dataclass
class Stream:
    config: StreamConfig
    state: State
    created: str
    mirror: Optional[MirrorInfo] = None
    sources: Optional[List[SourceInfo]] = None
    cluster: Optional[Cluster] = None

    def __post_init__(self):
        if isinstance(self.config, dict):
            self.config = StreamConfig(**self.config)
        if isinstance(self.mirror, dict):
            self.mirror = MirrorInfo(**self.mirror)
        if self.sources:
            self.sources = [
                SourceInfo(**item) if isinstance(item, dict) else item
                for item in self.sources
            ]


@dataclass
class StreamInfoResponse(Stream, JetStreamResponse):
    pass


@dataclass
class StreamCreateResponse(Stream, JetStreamResponse):
    did_create: Optional[bool] = None


@dataclass
class StreamListResponse(JetStreamResponse):
    total: int
    offset: int
    limit: int
    streams: List[Stream] = field(default_factory=list)

    def __post_init__(self):
        if self.streams is None:
            self.streams = []
        self.streams = [
            Stream(**item) if isinstance(item, dict) else item
            for item in self.streams
        ]


@dataclass
class StreamNamesResponse(JetStreamResponse):
    total: int
    offset: int
    limit: int
    streams: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.streams is None:
            self.streams = []


@dataclass
class StreamDeleteResponse(JetStreamResponse):
    success: bool


@dataclass
class StreamMsgGetResponse(JetStreamResponse):
    message: Message

    def __post_init__(self):
        if isinstance(self.message, dict):
            self.message = Message(**self.message)


@dataclass
class StreamMsgDeleteResponse(JetStreamResponse):
    success: bool


@dataclass
class StreamPurgeResponse(JetStreamResponse):
    success: bool
    purged: int


@dataclass
class StreamSnapshotResponse(JetStreamResponse):
    config: StreamConfig
    state: State

    def __post_init__(self):
        if isinstance(self.config, dict):
            self.config = StreamConfig(**self.config)


@dataclass
class StreamCreateRequest:
    name: Optional[str]
    subjects: Optional[List[str]]
    retention: Retention
    max_consumers: int
    max_msgs: int
    max_msgs_per_subject: int
    max_bytes: int
    max_age: int
    max_msg_size: int
    storage: Storage
    num_replicas: int
    no_ack: Optional[bool] = False
    template_owner: Optional[str] = None
    discard: Optional[Discard] = Discard.old
    duplicate_window: Optional[int] = 0
    placement: Optional[Placement] = None
    mirror: Optional[Mirror] = None
    sources: Optional[List[Source]] = None

    def __post_init__(self):
        if isinstance(self.mirror, dict):
            self.mirror = Mirror(**self.mirror)
        if isinstance(self.placement, dict):
            self.placement = Placement(**self.placement)
        if self.sources:
            self.sources = [
                Source(**item) if isinstance(item, dict) else item
                for item in self.sources
            ]


@dataclass
class StreamUpdateRequest(StreamCreateRequest):
    pass


@dataclass
class StreamInfoRequest:
    deleted_details: Optional[bool] = None


@dataclass
class StreamListRequest:
    offset: Optional[int] = None


@dataclass
class StreamNamesRequest:
    offset: Optional[int] = None
    subject: Optional[str] = None


@dataclass
class StreamMsgGetRequest:
    seq: Optional[int] = None
    last_by_subj: Optional[str] = None

    def __post_init__(self):
        """Ensure exactly 1 parameter is set between 'seq' and 'last_by_subj'"""
        if self.seq and self.last_by_subj:
            raise ValueError(
                "Both 'seq' and 'last_by_subj' arguments cannot be specified at same time"
            )
        if self.seq is None and self.last_by_subj is None:
            raise ValueError(
                "Either 'seq' or 'last_by_subj' argument must be specified."
            )


@dataclass
class StreamMsgDeleteRequest:
    seq: int
    no_erase: Optional[bool] = None


@dataclass
class StreamPurgeRequest:
    filter: Optional[str] = None
    seq: Optional[int] = None
    keep: Optional[int] = None


@dataclass
class StreamSnapshotRequest:
    deliver_subject: str
    no_consumers: Optional[bool] = None
    chunk_size: Optional[int] = None
    jsck: Optional[bool] = False
