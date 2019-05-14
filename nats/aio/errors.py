# Copyright 2016-2018 The NATS Authors
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

STALE_CONNECTION = b"'Stale Connection'"
AUTHORIZATION_VIOLATION = b"'Authorization Violation'"


class NatsError(Exception):
    pass


class ErrConnectionClosed(NatsError):
    def __str__(self):
        return "nats: Connection Closed"


class ErrSecureConnRequired(NatsError):
    def __str__(self):
        return "nats: Secure Connection required"


class ErrSecureConnWanted(NatsError):
    def __str__(self):
        return "nats: Secure Connection not available"


class ErrSecureConnFailed(NatsError):
    def __str__(self):
        return "nats: Secure Connection failed"


class ErrBadSubscription(NatsError):
    def __str__(self):
        return "nats: Invalid Subscription"


class ErrBadSubject(NatsError):
    def __str__(self):
        return "nats: Invalid Subject"


class ErrSlowConsumer(NatsError):
    def __init__(self, subject=None, sid=None):
        self.subject = subject
        self.sid = sid

    def __str__(self):
        return "nats: Slow Consumer, messages dropped"


class ErrTimeout(asyncio.TimeoutError):
    def __str__(self):
        return "nats: Timeout"


class ErrBadTimeout(NatsError):
    def __str__(self):
        return "nats: Timeout Invalid"


class ErrAuthorization(NatsError):
    def __str__(self):
        return "nats: Authorization Failed"


class ErrNoServers(NatsError):
    def __str__(self):
        return "nats: No servers available for connection"


class ErrJsonParse(NatsError):
    def __str__(self):
        return "nats: Connect message, json parse err"


class ErrStaleConnection(NatsError):
    def __str__(self):
        return "nats: Stale Connection"


class ErrMaxPayload(NatsError):
    def __str__(self):
        return "nats: Maximum Payload Exceeded"


class ErrDrainTimeout(ErrTimeout):
    def __str__(self):
        return "nats: Draining Connection Timed Out"


class ErrConnectionDraining(NatsError):
    def __str__(self):
        return "nats: Connection Draining"


class ErrConnectionReconnecting(NatsError):
    def __str__(self):
        return "nats: Connection Reconnecting"


class ErrInvalidUserCredentials(NatsError):
    def __str__(self):
        return "nats: Invalid user credentials"
