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

import abc

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Awaitable, Callable, TypeAlias, Optional

from nats.aio.msg import Msg
from nats.micro.api import ERROR_HEADER, ERROR_CODE_HEADER


class Request:
    _msg: Msg

    def __init__(self, msg: Msg) -> None:
        self._msg = msg

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
        self, data: bytes = b"", headers: Optional[Dict[str, str]] = None
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
        code: int | str,
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

        headers.update(
            {
                ERROR_HEADER: description,
                ERROR_CODE_HEADER: str(code),
            }
        )

        await self.respond(data, headers=headers)


Handler: TypeAlias = Callable[[Request], Awaitable[None]]
"""Handler is a function that processes a service request."""
