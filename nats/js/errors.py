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

from nats.aio.errors import NatsError

class JetStreamError(NatsError):
    def __str__(self):
        return "nats: JetStream Error"


class JetStreamAPIError(JetStreamError):
    def __init__(self, code=None, description=None, err_code=None):
        self.code = code
        self.err_code = err_code
        self.description = description

    def __str__(self):
        return f"nats: JetStream API Error: code={self.code} err_code={self.err_code} description='{self.description}'"


class NotJSMessageError(JetStreamError):
    def __str__(self):
        return "nats: not a JetStream message"

class NoStreamResponseError(JetStreamError):
    def __str__(self):
        return "nats: no response from stream"
