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

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nats.aio.subscription import Subscription


class Error(Exception):
    pass


class TimeoutError(asyncio.TimeoutError):

    def __str__(self) -> str:
        return "nats: timeout"


class NoRespondersError(Error):

    def __str__(self) -> str:
        return "nats: no responders available for request"


class StaleConnectionError(Error):

    def __str__(self) -> str:
        return "nats: stale connection"


class OutboundBufferLimitError(Error):

    def __str__(self) -> str:
        return "nats: outbound buffer limit exceeded"


class UnexpectedEOF(StaleConnectionError):

    def __str__(self) -> str:
        return "nats: unexpected EOF"


class FlushTimeoutError(TimeoutError):

    def __str__(self) -> str:
        return "nats: flush timeout"


class ConnectionClosedError(Error):

    def __str__(self) -> str:
        return "nats: connection closed"


class SecureConnRequiredError(Error):

    def __str__(self) -> str:
        return "nats: secure connection required"


class SecureConnWantedError(Error):

    def __str__(self) -> str:
        return "nats: secure connection not available"


class SecureConnFailedError(Error):

    def __str__(self) -> str:
        return "nats: secure connection failed"


class BadSubscriptionError(Error):

    def __str__(self) -> str:
        return "nats: invalid subscription"


class BadSubjectError(Error):

    def __str__(self) -> str:
        return "nats: invalid subject"


class SlowConsumerError(Error):

    def __init__(
        self, subject: str, reply: str, sid: int, sub: Subscription
    ) -> None:
        self.subject = subject
        self.reply = reply
        self.sid = sid
        self.sub = sub

    def __str__(self) -> str:
        return "nats: slow consumer, messages dropped subject: " \
               f"{self.subject}, sid: {self.sid}, sub: {self.sub}"


class BadTimeoutError(Error):

    def __str__(self) -> str:
        return "nats: timeout invalid"


class AuthorizationError(Error):

    def __str__(self) -> str:
        return "nats: authorization failed"


class NoServersError(Error):

    def __str__(self) -> str:
        return "nats: no servers available for connection"


class JsonParseError(Error):

    def __str__(self) -> str:
        return "nats: connect message, json parse err"


class MaxPayloadError(Error):

    def __str__(self) -> str:
        return "nats: maximum payload exceeded"


class DrainTimeoutError(TimeoutError):

    def __str__(self) -> str:
        return "nats: draining connection timed out"


class ConnectionDrainingError(Error):

    def __str__(self) -> str:
        return "nats: connection draining"


class ConnectionReconnectingError(Error):

    def __str__(self) -> str:
        return "nats: connection reconnecting"


class InvalidUserCredentialsError(Error):

    def __str__(self) -> str:
        return "nats: invalid user credentials"


class InvalidCallbackTypeError(Error):

    def __str__(self) -> str:
        return "nats: callbacks must be coroutine functions"


class ProtocolError(Error):

    def __str__(self) -> str:
        return "nats: protocol error"


class NotJSMessageError(Error):
    """
    When it is attempted to use an API meant for JetStream on a message
    that does not belong to a stream.
    """

    def __str__(self) -> str:
        return "nats: not a JetStream message"


class MsgAlreadyAckdError(Error):

    def __init__(self, msg=None) -> None:
        self._msg = msg

    def __str__(self) -> str:
        return f"nats: message was already acknowledged: {self._msg}"
