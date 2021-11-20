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
from nats.js import api


class Error(nats.errors.Error):
    """
    An Error raised by the NATS client when using JetStream.
    """
    def __init__(self, description=None):
        self.description = description

    def __str__(self):
        desc = ''
        if self.description:
            desc = self.description
        return f"nats: JetStream.{self.__class__.__name__} {desc}"


class APIError(Error):
    """
    An error that is the result of interacting with NATS JetStream.
    """
    def __init__(
        self,
        code=None,
        description=None,
        err_code=None,
        stream=None,
        seq=None
    ):
        self.code = code
        self.err_code = err_code
        self.description = description
        self.stream = stream
        self.seq = seq

    @classmethod
    def from_msg(cls, msg):
        code = msg.header[api.StatusHdr]
        if code == api.ServiceUnavailableStatus:
            raise ServiceUnavailableError
        else:
            desc = msg.header[api.DescHdr]
            raise APIError(code=int(code), description=desc)

    @classmethod
    def from_error(cls, err):
        code = err['code']
        if code == 503:
            raise ServiceUnavailableError(**err)
        elif code == 500:
            raise ServerError(**err)
        elif code == 404:
            raise NotFoundError(**err)
        elif code == 400:
            raise BadRequestError(**err)
        else:
            raise APIError(**err)

    def __str__(self):
        return f"nats: {self.__class__.__name__}: code={self.code} err_code={self.err_code} description='{self.description}'"


class ServiceUnavailableError(APIError):
    """
    A 503 error
    """
    pass


class ServerError(APIError):
    """
    A 500 error
    """
    pass


class NotFoundError(APIError):
    """
    A 404 error
    """
    pass


class BadRequestError(APIError):
    """
    A 400 error
    """
    pass


class NotJSMessageError(Error):
    """
    When it is attempted to use an API meant for JetStream on a message
    xthat does not belong to a stream.
    """
    def __str__(self):
        return "nats: not a JetStream message"


class NoStreamResponseError(Error):
    """
    Raised if the client gets a 503 when publishing a message.
    """
    def __str__(self):
        return "nats: no response from stream"


class ConsumerSequenceMismatchError(Error):
    """
    Async error raised by the client with idle_heartbeat mode enabled
    when one of the message sequences is not the expected one.
    """
    def __init__(
        self,
        stream_resume_sequence=None,
        consumer_sequence=None,
        last_consumer_sequence=None
    ):
        self.stream_resume_sequence = stream_resume_sequence
        self.consumer_sequence = consumer_sequence
        self.last_consumer_sequence = last_consumer_sequence

    def __str__(self):
        gap = self.last_consumer_sequence - self.consumer_sequence
        return f"nats: sequence mismatch for consumer at sequence {self.consumer_sequence} ({gap} sequences behind), should restart consumer from stream sequence {self.stream_resume_sequence}"


class BucketNotFoundError(NotFoundError):
    """
    When attempted to bind to a JetStream KeyValue that does not exist.
    """
    pass


class BadBucketError(Error):
    pass


class KeyDeletedError(Error):
    """
    Raised when trying to get a key that was deleted from a JetStream KeyValue store.
    """
    def __init__(self, entry=None, op=None):
        self.entry = entry
        self.op = op

    def __str__(self):
        return "nats: key was deleted"
