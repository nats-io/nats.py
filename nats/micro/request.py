# Copyright 2021-2022 The NATS Authors
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

from __future__ import annotations

import abc

from dataclasses import dataclass
from enum import Enum
from nats.aio.msg import Msg
from nats.micro import api
from typing import Awaitable, Callable
from typing_extensions import TypeAlias

class Verb(str, Enum):
    PING = "PING"
    INFO = "INFO"
    STATS = "STATS"


def control_subject(
    verb: Verb,
    service: str | None,
    id: str | None,
    prefix: str = api.DEFAULT_PREFIX,
) -> str:
    """Get the internal subject for a verb."""
    verb_literal = verb.value
    if service:
        if id:
            return f"{prefix}.{verb_literal}.{service}.{id}"
        return f"{prefix}.{verb_literal}.{service}"
    return f"{prefix}.{verb_literal}"


class Request:
    msg: Msg

    def __init__(self, msg: Msg) -> None:
        self.msg = msg

    def subject(self) -> str:
        """The subject on which request was received."""
        return self.msg.subject

    def headers(self) -> dict[str, str]:
        """The headers of the request."""
        return self.msg.headers or {}

    def data(self) -> bytes:
        """The data of the request."""
        return self.msg.data

    async def respond(
        self, data: bytes, headers: dict[str, str] | None = None
    ) -> None:
        """Send a response to the request.

        :param data: The response data.
        :param headers: Additional response headers.
        """
        if not self.msg.reply:
            raise ValueError("no reply subject set")

        await self.msg._client.publish(
            self.msg.reply,
            data,
            headers=headers,
        )

    async def respond_error(
        self,
        code: int,
        description: str,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send an error response to the request.

        :param code: The error code describing the error.
        :param description: A string describing the error which can be displayed to the client.
        :param data: The error data.
        :param headers: Additional response headers.
        """
        if not headers:
            headers = {}
        headers["Nats-Service-Error"] = description
        headers["Nats-Service-Error-Code"] = str(code)
        await self.respond(data or b"", headers=headers)

Handler: TypeAlias = Callable[[Request], Awaitable[None]]
"""Handler is a function that processes a service request."""
