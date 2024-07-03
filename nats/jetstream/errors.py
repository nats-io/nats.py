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
from typing import Optional


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

    def __init__(
        self, message=None, code=None, error_code=None, description=None
    ):
        self.message = message
        self.code = code
        self.error_code = error_code
        self.description = description

    def __str__(self) -> str:
        return (
            f"nats: {type(self).__name__}: code={self.code} err_code={self.error_code} "
            f"description='{self.description}'"
        )


class StreamNameAlreadyInUseError(Error):
    pass


class StreamNotFoundError(Error):

    def __init__(self):
        super().__init__()


class StreamSubjectTransformNotSupportedError(Error):

    def __init__(self):
        super().__init__()



class StreamSourceNotSupportedError(Error):

    def __init__(self):
        super().__init__()


class StreamSourceMultipleFilterSubjectsNotSupported(Error):

    def __init__(self):
        super().__init__()


class MsgNotFoundError(Error):

    def __init__(self):
        super().__init__()

class NoStreamResponseError(Error):

    def __init__(self):
        super().__init__()


class InvalidResponseError(Error):

    def __init__(self):
        super().__init__()
