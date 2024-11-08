# Copyright 2021-2024 The NATS Authors
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

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from nats.aio.msg import Msg

ERROR_HEADER = "Nats-Service-Error"
ERROR_CODE_HEADER = "Nats-Service-Error-Code"


class Request:
    _msg: Msg

    def __init__(self, msg: Msg) -> None:
        self._msg = msg
        self._error = None

    @property
    def subject(self) -> str:
        """The subject on which request was received."""
        return self._msg.subject

    @property
    def headers(self) -> Optional[Dict[str, str]]:
        """The headers of the request."""
        return self._msg.headers

    @property
    def data(self) -> bytes:
        """The data of the request."""
        return self._msg.data

    async def respond(
        self,
        data: bytes = b"",
        headers: Optional[Dict[str, str]] = None
    ) -> None:
        """Send a response to the request.

        :param data: The response data.
        :param headers: Additional response headers.
        """
        if not self._msg.reply:
            raise ValueError("no reply subject set")

        await self._msg._client.publish(
            self._msg.reply,
            data,
            headers=headers,
        )

    async def respond_error(
        self,
        code: str,
        description: str,
        data: bytes = b"",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """Send an error response to the request.

        :param code: The error code describing the error.
        :param description: A string describing the error which can be displayed to the client.
        :param data: The error data.
        :param headers: Additional response headers.
        """
        if headers:
            headers = headers.copy()
        else:
            headers = {}

        headers.update({
            ERROR_HEADER: description,
            ERROR_CODE_HEADER: code,
        })

        await self.respond(data, headers=headers)


Handler = Callable[[Request], Awaitable[None]]
"""Handler is a function that processes a service request."""


@dataclass
class ServiceError(Exception):
    code: str
    description: str

    def __repr__(self) -> str:
        return f"{self.code}:{self.description}"

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "description": self.description}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ServiceError:
        return cls(
            code=data.get("code", ""), description=data.get("description", "")
        )
