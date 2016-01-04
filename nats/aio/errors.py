# Copyright 2015 Apcera Inc. All rights reserved.

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
