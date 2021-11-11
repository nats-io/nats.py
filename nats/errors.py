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

import asyncio


class Error(Exception):
    pass


class TimeoutError(asyncio.TimeoutError):
    def __str__(self):
        return "nats: timeout"


class ConnectionClosedError(Error):
    def __str__(self):
        return "nats: connection closed"


class SecureConnRequiredError(Error):
    def __str__(self):
        return "nats: secure connection required"


class SecureConnWantedError(Error):
    def __str__(self):
        return "nats: secure connection not available"


class SecureConnFailedError(Error):
    def __str__(self):
        return "nats: secure connection failed"


class BadSubscriptionError(Error):
    def __str__(self):
        return "nats: invalid subscription"


class BadSubjectError(Error):
    def __str__(self):
        return "nats: invalid subject"


class SlowConsumerError(Error):
    def __init__(self, subject=None, sid=None, sub=None):
        self.subject = subject
        self.sid = sid
        self.sub = sub

    def __str__(self):
        return "nats: slow consumer, messages dropped"


class BadTimeoutError(Error):
    def __str__(self):
        return "nats: timeout invalid"


class AuthorizationError(Error):
    def __str__(self):
        return "nats: authorization failed"


class NoServersError(Error):
    def __str__(self):
        return "nats: no servers available for connection"


class JsonParseError(Error):
    def __str__(self):
        return "nats: connect message, json parse err"


class StaleConnectionError(Error):
    def __str__(self):
        return "nats: stale connection"


class MaxPayloadError(Error):
    def __str__(self):
        return "nats: maximum payload exceeded"


class DrainTimeoutError(TimeoutError):
    def __str__(self):
        return "nats: draining connection timed out"


class ConnectionDrainingError(Error):
    def __str__(self):
        return "nats: connection draining"


class ConnectionReconnectingError(Error):
    def __str__(self):
        return "nats: connection reconnecting"


class InvalidUserCredentialsError(Error):
    def __str__(self):
        return "nats: invalid user credentials"


class InvalidCallbackTypeError(Error):
    def __str__(self):
        return "nats: callbacks must be coroutine functions"


class NoRespondersError(Error):
    def __str__(self):
        return "nats: no responders available for request"


class ProtocolError(Error):
    def __str__(self):
        return "nats: protocol error"
