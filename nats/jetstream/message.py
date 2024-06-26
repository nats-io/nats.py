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

import nats.aio.msg

from dataclasses import dataclass, field
from enum import Enum


class Header(str, Enum):
    """
    Provides known headers that can be used to control message behavior.
    """

    MSG_ID = "Nats-Msg-Id"
    """Used to specify a user-defined message ID. It can be used
    e.g. for deduplication in conjunction with the Duplicates duration on
    ConsumerConfig or to provide optimistic concurrency safety together with
    ExpectedLastMsgID.

    This can be set when publishing messages using id option.
    """

    EXPECTED_STREAM = "Nats-Expected-Stream"
    """Contains stream name and is used to assure that the
    published message is received by the expected stream. The server will reject the
    message if it is not the case.

    This can be set when publishing messages using expect_stream option.
    """

    EXPECTED_LAST_SEQUENCE = "Nats-Expected-Last-Sequence"
    """Contains the expected last sequence number of the
    stream and can be used to apply optimistic concurrency control at the stream
    level. The server will reject the message if it is not the case.

    This can be set when publishing messages using expected_last_sequence
    option.
    """

    EXPECTED_LAST_SUBJECT_SEQEQUENCE = "Nats-Expected-Last-Subject-Sequence"
    """Contains the expected last sequence number on
    the subject and can be used to apply optimistic concurrency control at
    the subject level. The server will reject the message if it is not the case.

    This can be set when publishing messages using expected_last_subject_sequence option.
    """

    EXPECTED_LAST_MSG_ID = "Nats-Expected-Last-Msg-Id"
    """Contains the expected last message ID on the
    subject and can be used to apply optimistic concurrency control at
    the stream level. The server will reject the message if it is not the case.

    This can be set when publishing messages using WithExpectLastMsgID
    option.
    """

    ROLLUP = "Nats-Rollup"
    """Used to apply a purge of all prior messages in the stream
    ("all") or at the subject ("sub") before this message.
    """

    STREAM = "Nats-Stream"
    """Contains the stream name the message was republished from or
    the stream name the message was retrieved from using direct get.
    """

    SEQUENCE = "Nats-Sequence"
    """
    Contains the original sequence number of the message.
    """

    TIMESTAMP = "Nats-Time-Stamp"
    """
    Contains the original timestamp of the message.
    """

    SUBJECT = "Nats-Subject"
    """
    Contains the original subject the message was published to.
    """

    LAST_SEQUENCE = "Nats-Last-Sequence"
    """
    Contains the last sequence of the message having the
    same subject, otherwise zero if this is the first message for the
    subject.
    """


class Status(str, Enum):
    SERVICE_UNAVAILABLE = "503"
    NO_MESSAGES = "404"
    REQUEST_TIMEOUT = "408"
    CONFLICT = "409"
    CONTROL_MESSAGE = "100"


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


# FIXME
# For now, we will use the message class from the nats.aio.msg module.
# This needs to be fixed before releasing.
Msg = nats.aio.msg.Msg
