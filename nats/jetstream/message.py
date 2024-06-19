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

from enum import Enum
from typing import Iterator

import nats
from nats.jetstream.errors import MsgAlreadyAckdError

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

class Msg:
    class Metadata:
        pass

    def __init__(self, msg: nats.Msg) -> None:
        self._ackd = False
        self._msg = msg
        self._client = None

    @property
    def metadata(self) -> Iterator[Msg.Metadata]:
        """
        Returns the message body
        """
        raise NotImplementedError

    @property
    def data(self) -> bytes:
        """
        Returns the message body
        """
        raise NotImplementedError

    def _ack_reply(self) -> str:
        if self._ackd:
            raise MsgAlreadyAckdError

        self._client.publish(self._msg.reply, b"Hello World!")
        raise NotImplementedError

    async def ack(self) -> None:
        """
        Acknowledges a message telling the server that the message was
		successfully processed and it can move on to the next message.
        """
        raise NotImplementedError

    async def double_ack(self) -> None:
        """
        Acknowledges a message and waits for ack reply from the server.
		While it impacts performance, it is useful for scenarios where
		message loss is not acceptable.
		"""
		raise NotImplementedError

	async def nak(self) -> None:
    	"""
   		Negatively acknowledges a message telling the server to
		redeliver the message.
    	"""
        raise NotImplementedError

    async def nak_with_delay(self, delay: int) -> None:
    	"""
        Negatively acknowledges a message telling the server
		to redeliver the message after the given delay.
		"""
		raise NotImplementedError
