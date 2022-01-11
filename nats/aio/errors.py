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
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.DrainTimeoutError` instead.
    """
    pass


class ErrInvalidUserCredentials(nats.errors.InvalidUserCredentialsError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.InvalidUserCredentialsError` instead.
    """
    pass


class ErrInvalidCallbackType(nats.errors.InvalidCallbackTypeError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.InvalidCallbackTypeError` instead.
    """
    pass


class ErrConnectionReconnecting(nats.errors.ConnectionReconnectingError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.ConnectionReconnectingError` instead.
    """
    pass


class ErrConnectionDraining(nats.errors.ConnectionDrainingError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.ConnectionDrainingError` instead.
    """
    pass


class ErrMaxPayload(nats.errors.MaxPayloadError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.MaxPayloadError` instead.
    """
    pass


class ErrStaleConnection(nats.errors.StaleConnectionError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.StaleConnectionError` instead.
    """
    pass


class ErrJsonParse(nats.errors.JsonParseError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.JsonParseError` instead.
    """
    pass


class ErrSecureConnRequired(nats.errors.SecureConnRequiredError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.SecureConnRequiredError` instead.
    """
    pass


class ErrSecureConnWanted(nats.errors.SecureConnWantedError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.SecureConnWantedError` instead.
    """
    pass


class ErrSecureConnFailed(nats.errors.SecureConnFailedError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.SecureConnFailedError` instead.
    """
    pass


class ErrBadSubscription(nats.errors.BadSubscriptionError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.BadSubscriptionError` instead.
    """
    pass


class ErrBadSubject(nats.errors.BadSubjectError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.BadSubjectError` instead.
    """
    pass


class ErrSlowConsumer(nats.errors.SlowConsumerError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.SlowConsumerError` instead.
    """
    pass


class ErrTimeout(nats.errors.TimeoutError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.TimeoutError` instead.
    """
    pass


class ErrBadTimeout(nats.errors.BadTimeoutError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.BadTimeoutError` instead.
    """
    pass


class ErrAuthorization(nats.errors.AuthorizationError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.AuthorizationError` instead.
    """
    pass


class ErrNoServers(nats.errors.NoServersError):
    """

    .. deprecated:: v2.0.0

    Please use `nats.errors.NoServersError` instead.
    """
    pass
