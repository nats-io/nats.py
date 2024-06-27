# Copyright 2016-2024 The NATS Authors
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

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional


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
    consumer: int = field(metadata={'json': 'consumer_seq'})
    """Consumer sequence number."""

    stream: int = field(metadata={'json': 'stream_seq'})
    """Stream sequence number."""

    last_active: Optional[datetime] = field(
        default=None, metadata={'json': 'last_active'}
    )
    """Last activity timestamp."""


@dataclass
class ConsumerConfig:
    """
    ConsumerConfig is the configuration of a JetStream consumer.
    """
    name: Optional[str] = field(default=None, metadata={'json': 'name'})
    """Optional name for the consumer."""

    durable: Optional[str] = field(
        default=None, metadata={'json': 'durable_name'}
    )
    """Optional durable name for the consumer."""

    description: Optional[str] = field(
        default=None, metadata={'json': 'description'}
    )
    """Optional description of the consumer."""

    deliver_policy: DeliverPolicy = field(
        default=DeliverPolicy.ALL, metadata={'json': 'deliver_policy'}
    )
    """Defines from which point to start delivering messages from the stream. Defaults to DeliverAllPolicy."""

    opt_start_seq: Optional[int] = field(
        default=None, metadata={'json': 'opt_start_seq'}
    )
    """Optional sequence number from which to start message delivery."""

    opt_start_time: Optional[datetime] = field(
        default=None, metadata={'json': 'opt_start_time'}
    )
    """Optional time from which to start message delivery."""

    ack_policy: AckPolicy = field(
        default=AckPolicy.EXPLICIT, metadata={'json': 'ack_policy'}
    )
    """Defines the acknowledgement policy for the consumer. Defaults to AckExplicitPolicy."""

    ack_wait: Optional[timedelta] = field(
        default=None, metadata={'json': 'ack_wait'}
    )
    """How long the server will wait for an acknowledgement before resending a message."""

    max_deliver: Optional[int] = field(
        default=None, metadata={'json': 'max_deliver'}
    )
    """Maximum number of delivery attempts for a message."""

    backoff: Optional[List[timedelta]] = field(
        default=None, metadata={'json': 'backoff'}
    )
    """Optional back-off intervals for retrying message delivery after a failed acknowledgement."""

    filter_subject: Optional[str] = field(
        default=None, metadata={'json': 'filter_subject'}
    )
    """Can be used to filter messages delivered from the stream."""

    replay_policy: ReplayPolicy = field(
        default=ReplayPolicy.INSTANT, metadata={'json': 'replay_policy'}
    )
    """Defines the rate at which messages are sent to the consumer."""

    rate_limit: Optional[int] = field(
        default=None, metadata={'json': 'rate_limit_bps'}
    )
    """Optional maximum rate of message delivery in bits per second."""

    sample_frequency: Optional[str] = field(
        default=None, metadata={'json': 'sample_freq'}
    )
    """Optional frequency for sampling how often acknowledgements are sampled for observability."""

    max_waiting: Optional[int] = field(
        default=None, metadata={'json': 'max_waiting'}
    )
    """Maximum number of pull requests waiting to be fulfilled."""

    max_ack_pending: Optional[int] = field(
        default=None, metadata={'json': 'max_ack_pending'}
    )
    """Maximum number of outstanding unacknowledged messages."""

    headers_only: Optional[bool] = field(
        default=None, metadata={'json': 'headers_only'}
    )
    """Indicates whether only headers of messages should be sent."""

    max_request_batch: Optional[int] = field(
        default=None, metadata={'json': 'max_batch'}
    )
    """Optional maximum batch size a single pull request can make."""

    max_request_expires: Optional[timedelta] = field(
        default=None, metadata={'json': 'max_expires'}
    )
    """Maximum duration a single pull request will wait for messages to be available to pull."""

    max_request_max_bytes: Optional[int] = field(
        default=None, metadata={'json': 'max_bytes'}
    )
    """Optional maximum total bytes that can be requested in a given batch."""

    inactive_threshold: Optional[timedelta] = field(
        default=None, metadata={'json': 'inactive_threshold'}
    )
    """Duration which instructs the server to clean up the consumer if it has been inactive."""

    replicas: Optional[int] = field(default=None, metadata={'json': 'num_replicas'})
    """Number of replicas for the consumer's state."""

    memory_storage: Optional[bool] = field(
        default=None, metadata={'json': 'mem_storage'}
    )
    """Flag to force the consumer to use memory storage."""

    filter_subjects: Optional[List[str]] = field(
        default=None, metadata={'json': 'filter_subjects'}
    )
    """Allows filtering messages from a stream by subject."""

    metadata: Optional[Dict[str, str]] = field(
        default=None, metadata={'json': 'metadata'}
    )
    """Set of application-defined key-value pairs for associating metadata on the consumer."""


@dataclass
class ConsumerInfo:
    """
    ConsumerInfo is the detailed information about a JetStream consumer.
    """
    stream: str = field(metadata={'json': 'stream_name'})
    """Name of the stream that the consumer is bound to."""

    name: str = field(metadata={'json': 'name'})
    """Unique identifier for the consumer."""

    created: datetime = field(metadata={'json': 'created'})
    """Timestamp when the consumer was created."""

    config: ConsumerConfig = field(metadata={'json': 'config'})
    """Configuration settings of the consumer."""

    delivered: SequenceInfo = field(metadata={'json': 'delivered'})
    """Information about the most recently delivered message."""

    ack_floor: SequenceInfo = field(metadata={'json': 'ack_floor'})
    """Indicates the message before the first unacknowledged message."""

    num_ack_pending: int = field(metadata={'json': 'num_ack_pending'})
    """Number of messages that have been delivered but not yet acknowledged."""

    num_redelivered: int = field(metadata={'json': 'num_redelivered'})
    """Counts the number of messages that have been redelivered and not yet acknowledged."""

    num_waiting: int = field(metadata={'json': 'num_waiting'})
    """Count of active pull requests."""

    num_pending: int = field(metadata={'json': 'num_pending'})
    """Number of messages that match the consumer's filter but have not been delivered yet."""

    timestamp: datetime = field(metadata={'json': 'ts'})
    """Timestamp when the info was gathered by the server."""

    push_bound: bool = field(default=False, metadata={'json': 'push_bound'})
    """Indicates whether at least one subscription exists for the delivery subject of this consumer."""

    cluster: Optional[ClusterInfo] = field(
        default=None, metadata={'json': 'cluster'}
    )
    """Information about the cluster to which this consumer belongs."""


@dataclass
class OrderedConsumerConfig:
    """
    OrderedConsumerConfig is the configuration of an ordered JetStream consumer.
    """
    filter_subjects: Optional[List[str]] = field(
        default=None, metadata={'json': 'filter_subjects'}
    )
    """Allows filtering messages from a stream by subject."""

    deliver_policy: DeliverPolicy = field(metadata={'json': 'deliver_policy'})
    """Defines from which point to start delivering messages from the stream."""

    opt_start_seq: Optional[int] = field(
        default=None, metadata={'json': 'opt_start_seq'}
    )
    """Optional sequence number from which to start message delivery."""

    opt_start_time: Optional[datetime] = field(
        default=None, metadata={'json': 'opt_start_time'}
    )
    """Optional time from which to start message delivery."""

    replay_policy: ReplayPolicy = field(metadata={'json': 'replay_policy'})
    """Defines the rate at which messages are sent to the consumer."""

    inactive_threshold: Optional[timedelta] = field(
        default=None, metadata={'json': 'inactive_threshold'}
    )
    """Duration which instructs the server to clean up the consumer if it has been inactive."""

    headers_only: Optional[bool] = field(
        default=None, metadata={'json': 'headers_only'}
    )
    """Indicates whether only headers of messages should be sent."""

    max_reset_attempts: Optional[int] = field(
        default=None, metadata={'json': 'max_reset_attempts'}
    )
    """Maximum number of attempts for the consumer to be recreated in a single recreation cycle."""


class Consumer:
    raise NotImplementedError


class PullConsumer(Consumer):
    raise NotImplementedError


class OrderedConsumer(Consumer):
    raise NotImplementedError


class StreamConsumerManager:
    """
    Provides methods for directly managing consumers.
    """

    async def create_or_update_consumer(
        self,
        stream: str,
        config: ConsumerConfig,
        timeout: Optional[float] = None
    ) -> Consumer:
        """
        CreateOrUpdateConsumer creates a consumer on a given stream with
        given config. If consumer already exists, it will be updated (if
        possible). Consumer interface is returned, allowing to operate on a
        consumer (e.g. fetch messages).
        """
        raise NotImplementedError

    async def create_consumer(
        self,
        stream: str,
        config: ConsumerConfig,
        timeout: Optional[float] = None
    ) -> Consumer:
        """
        CreateConsumer creates a consumer on a given stream with given
        config. If consumer already exists and the provided configuration
        differs from its configuration, ErrConsumerExists is returned. If the
        provided configuration is the same as the existing consumer, the
        existing consumer is returned. Consumer interface is returned,
        allowing to operate on a consumer (e.g. fetch messages).
        """
        raise NotImplementedError

    async def update_consumer(
        self,
        stream: str,
        config: ConsumerConfig,
        timeout: Optional[float] = None
    ) -> Consumer:
        """
        Updates an existing consumer.

        If consumer does not exist, an error is raised.
        """
        raise NotImplementedError

    async def ordered_consumer(
        self,
        stream: str,
        config: OrderedConsumerConfig,
        timeout: Optional[float] = None
    ) -> Consumer:
        """
        Returns returns an instance of an ordered consumer.

        Ordered consumers  are managed by the library and provide a simple way to consume
        messages from a stream.

        Ordered consumers are ephemeral in-memory pull consumers and are resilient to deletes and restarts.
        """
        raise NotImplementedError

    async def consumer(
        self,
        stream: str,
        consumer: str,
        timeout: Optional[float] = None
    ) -> Consumer:
        """
        Returns an instance of an existing consumer, allowing processing of messages.

        If consumer does not exist, an error is raised.
        """
        raise NotImplementedError

    async def delete_consumer(
        self,
        stream: str,
        consumer: str,
        timeout: Optional[float] = None
    ) -> None:
        """
        Removes a consumer with given name from a stream.
        If consumer does not exist, an error is raised.
        """
        raise NotImplementedError
