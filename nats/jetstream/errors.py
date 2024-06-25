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

from typing import Optional
from enum import Enum

class ErrorCode(Enum):
    JETSTREAM_NOT_ENABLED_FOR_ACCOUNT = 10039
    JETSTREAM_NOT_ENABLED = 10076
    STREAM_NOT_FOUND = 10059
    STREAM_NAME_IN_USE = 10058
    CONSUMER_CREATE = 10012
    CONSUMER_NOT_FOUND = 10014
    CONSUMER_NAME_EXISTS = 10013
    CONSUMER_ALREADY_EXISTS = 10105
    CONSUMER_EXISTS = 10148
    DUPLICATE_FILTER_SUBJECTS = 10136
    OVERLAPPING_FILTER_SUBJECTS = 10138
    CONSUMER_EMPTY_FILTER = 10139
    CONSUMER_DOES_NOT_EXIST = 10149
    MESSAGE_NOT_FOUND = 10037
    BAD_REQUEST = 10003
    STREAM_WRONG_LAST_SEQUENCE = 10071


class Error(Exception):
    def __init__(self, message=None, code=None, error_code=None, description=None):
        self.message = message
        self.code = code
        self.error_code = error_code
        self.description = description

    def __str__(self):
        if self.description:
            return f"nats: API error: code={self.code} err_code={self.error_code} description={self.description}"
        return f"nats: {self.message}"


class JetStreamNotEnabledError(Error):
    def __init__(self):
        super().__init__(
            message="jetstream not enabled",
            code=503,
            error_code=ErrorCode.JETSTREAM_NOT_ENABLED,
            description="jetstream not enabled",
        )


class JetStreamNotEnabledForAccountError(Error):
    def __init__(self):
        super().__init__(
            message="jetstream not enabled for account",
            code=503,
            error_code=ErrorCode.JETSTREAM_NOT_ENABLED_FOR_ACCOUNT,
            description="jetstream not enabled for account",
        )


class StreamNotFoundError(Error):
    def __init__(self):
        super().__init__(
            message="stream not found",
            code=404,
            error_code=ErrorCode.STREAM_NOT_FOUND,
            description="stream not found",
        )


class StreamNameAlreadyInUseError(Error):
    def __init__(self):
        super().__init__(
            message="stream name already in use",
            code=400,
            error_code=ErrorCode.STREAM_NAME_IN_USE,
            description="stream name already in use",
        )


class StreamSubjectTransformNotSupportedError(Error):
    def __init__(self):
        super().__init__(message="stream subject transformation not supported by nats-server")


class StreamSourceSubjectTransformNotSupportedError(Error):
    def __init__(self):
        super().__init__(message="stream subject transformation not supported by nats-server")


class StreamSourceNotSupportedError(Error):
    def __init__(self):
        super().__init__(message="stream sourcing is not supported by nats-server")


class StreamSourceMultipleFilterSubjectsNotSupportedError(Error):
    def __init__(self):
        super().__init__(message="stream sourcing with multiple subject filters not supported by nats-server")


class ConsumerNotFoundError(Error):
    def __init__(self):
        super().__init__(
            message="consumer not found",
            code=404,
            error_code=ErrorCode.CONSUMER_NOT_FOUND,
            description="consumer not found",
        )


class ConsumerExistsError(Error):
    def __init__(self):
        super().__init__(
            message="consumer already exists",
            code=400,
            error_code=ErrorCode.CONSUMER_EXISTS,
            description="consumer already exists",
        )


class ConsumerDoesNotExistError(Error):
    def __init__(self):
        super().__init__(
            message="consumer does not exist",
            code=400,
            error_code=ErrorCode.CONSUMER_DOES_NOT_EXIST,
            description="consumer does not exist",
        )


class MessageNotFoundError(Error):
    def __init__(self):
        super().__init__(
            message="message not found",
            code=404,
            error_code=ErrorCode.MESSAGE_NOT_FOUND,
            description="message not found",
        )


class BadRequestError(Error):
    def __init__(self):
        super().__init__(
            message="bad request",
            code=400,
            error_code=ErrorCode.BAD_REQUEST,
            description="bad request",
        )


class ConsumerCreateError(Error):
    def __init__(self):
        super().__init__(
            message="could not create consumer",
            code=500,
            error_code=ErrorCode.CONSUMER_CREATE,
            description="could not create consumer",
        )


class DuplicateFilterSubjectsError(Error):
    def __init__(self):
        super().__init__(
            message="consumer cannot have both FilterSubject and FilterSubjects specified",
            code=500,
            error_code=ErrorCode.DUPLICATE_FILTER_SUBJECTS,
            description="consumer cannot have both FilterSubject and FilterSubjects specified",
        )


class OverlappingFilterSubjectsError(Error):
    def __init__(self):
        super().__init__(
            message="consumer subject filters cannot overlap",
            code=500,
            error_code=ErrorCode.OVERLAPPING_FILTER_SUBJECTS,
            description="consumer subject filters cannot overlap",
        )


class EmptyFilterError(Error):
    def __init__(self):
        super().__init__(
            message="consumer filter in FilterSubjects cannot be empty",
            code=500,
            error_code=ErrorCode.CONSUMER_EMPTY_FILTER,
            description="consumer filter in FilterSubjects cannot be empty",
        )


class ConsumerMultipleFilterSubjectsNotSupportedError(Error):
    def __init__(self):
        super().__init__(message="multiple consumer filter subjects not supported by nats-server")


class ConsumerNameAlreadyInUseError(Error):
    def __init__(self):
        super().__init__(message="consumer name already in use")


class InvalidJSAckError(Error):
    def __init__(self):
        super().__init__(message="invalid jetstream publish response")


class StreamNameRequiredError(Error):
    def __init__(self):
        super().__init__(message="stream name is required")


class MsgAlreadyAckdError(Error):
    def __init__(self):
        super().__init__(message="message was already acknowledged")


class NoStreamResponseError(Error):
    def __init__(self):
        super().__init__(message="no response from stream")


class NotJSMessageError(Error):
    def __init__(self):
        super().__init__(message="not a jetstream message")


class InvalidStreamNameError(Error):
    def __init__(self):
        super().__init__(message="invalid stream name")


class InvalidSubjectError(Error):
    def __init__(self):
        super().__init__(message="invalid subject name")


class InvalidConsumerNameError(Error):
    def __init__(self):
        super().__init__(message="invalid consumer name")


class NoMessagesError(Error):
    def __init__(self):
        super().__init__(message="no messages")


class MaxBytesExceededError(Error):
    def __init__(self):
        super().__init__(message="message size exceeds max bytes")


class ConsumerDeletedError(Error):
    def __init__(self):
        super().__init__(message="consumer deleted")


class ConsumerLeadershipChangedError(Error):
    def __init__(self):
        super().__init__(message="leadership change")


class HandlerRequiredError(Error):
    def __init__(self):
        super().__init__(message="handler cannot be empty")


class EndOfDataError(Error):
    def __init__(self):
        super().__init__(message="end of data reached")


class NoHeartbeatError(Error):
    def __init__(self):
        super().__init__(message="no heartbeat received")


class ConsumerHasActiveSubscriptionError(Error):
    def __init__(self):
        super().__init__(message="consumer has active subscription")


class MsgNotBoundError(Error):
    def __init__(self):
        super().__init__(message="message is not bound to subscription/connection")


class MsgNoReplyError(Error):
    def __init__(self):
        super().__init__(message="message does not have a reply")


class MsgDeleteUnsuccessfulError(Error):
    def __init__(self):
        super().__init__(message="message deletion unsuccessful")


class AsyncPublishReplySubjectSetError(Error):
    def __init__(self):
        super().__init__(message="reply subject should be empty")


class TooManyStalledMsgsError(Error):
    def __init__(self):
        super().__init__(message="stalled with too many outstanding async published messages")


class InvalidOptionError(Error):
    def __init__(self):
        super().__init__(message="invalid jetstream option")
