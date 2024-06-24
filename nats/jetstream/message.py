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

from enum import Enum
from dataclasses import dataclass, field

class Header(str, Enum):
    """
    Provides known headers that can be used to control message behavior.
    """
    MSG_ID = "Nats-Msg-Id"
    EXPECTED_STREAM = "Nats-Expected-Stream"
    EXPECTED_LAST_SEQ = "Nats-Expected-Last-Sequence"
    EXPECTED_LAST_MSG_ID = "Nats-Expected-Last-Msg-Id"
    EXPECTED_LAST_SUBJECT_SEQUENCE = "Nats-Expected-Last-Subject-Sequence"

@dataclass
class SequencePair:
    """
    Provides a pair of the consumer and stream sequence numbers for a message.
    """

    consumer: int = field(metadata={"json": "consumer_seq"})
    """
    The consumer sequence number for message deliveries.
    This is the total number of messages the consumer has seen (including redeliveries).
    """

    stream: int = field(metadata={"json": "stream_seq"})
    """
    The stream sequence number for a message.
    """
