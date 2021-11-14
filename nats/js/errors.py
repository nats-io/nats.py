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
    def __init__(self, description=None):
        self.description = description

    def __str__(self):
        return f"nats: JetStream Error: {self.description}"


class APIError(Error):
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
    503 error
    """
    pass


class ServerError(APIError):
    """
    500 error
    """
    pass


class NotFoundError(APIError):
    """
    404 error
    """
    pass


class BadRequestError(APIError):
    """
    400 error
    """
    pass


class NotJSMessageError(Error):
    def __str__(self):
        return "nats: not a JetStream message"


class NoStreamResponseError(Error):
    def __str__(self):
        return "nats: no response from stream"
