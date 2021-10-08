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

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

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
    """Discard policy when a stream reaches its limits"""

    old = "old"
    new = "new"

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
