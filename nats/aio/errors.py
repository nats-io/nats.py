# Copyright 2016-2021 The NATS Authors
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

import nats.errors


class NatsError(nats.errors.Error):
    """
    .. deprecated:: v2.0.0


    Please use `nats.errors.Error` instead.
    """
    pass


class ErrConnectionClosed(nats.errors.ConnectionClosedError):
    """
    
    .. deprecated:: v2.0.0

    Please use `nats.errors.ConnectionClosedError` instead.
    """
    pass


class ErrDrainTimeout(nats.errors.DrainTimeoutError):
    pass


class ErrInvalidUserCredentials(nats.errors.InvalidUserCredentialsError):
    pass


class ErrInvalidCallbackType(nats.errors.InvalidCallbackTypeError):
    pass


class ErrConnectionReconnecting(nats.errors.ConnectionReconnectingError):
    pass


class ErrConnectionDraining(nats.errors.ConnectionDrainingError):
    pass


class ErrMaxPayload(nats.errors.MaxPayloadError):
    pass


class ErrStaleConnection(nats.errors.StaleConnectionError):
    pass


class ErrJsonParse(nats.errors.JsonParseError):
    pass


class ErrSecureConnRequired(nats.errors.SecureConnRequiredError):
    pass


class ErrSecureConnWanted(nats.errors.SecureConnWantedError):
    pass


class ErrSecureConnFailed(nats.errors.SecureConnFailedError):
    pass


class ErrBadSubscription(nats.errors.BadSubscriptionError):
    pass


class ErrBadSubject(nats.errors.BadSubjectError):
    pass


class ErrSlowConsumer(nats.errors.SlowConsumerError):
    pass


class ErrTimeout(nats.errors.TimeoutError):
    pass


class ErrBadTimeout(nats.errors.BadTimeoutError):
    pass


class ErrAuthorization(nats.errors.AuthorizationError):
    pass


class ErrNoServers(nats.errors.NoServersError):
    pass
