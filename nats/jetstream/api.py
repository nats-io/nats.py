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

from __future__ import annotations

import json
from dataclasses import MISSING, dataclass, field, fields, asdict, is_dataclass
from typing import (
    Any,
    Dict,
    Optional,
    Protocol,
    Self,
    Type,
    TypeVar,
    get_args,
    get_origin,
)

import nats

from .message import Msg

T = TypeVar("T", bound="Base")

DEFAULT_PREFIX = "$JS.API"
INBOX_PREFIX = b'_INBOX.'

@dataclass
class Base:
    """
    Provides methods for converting to and from json.
    """

    def to_dict(self, include_null=False) -> dict:
        """
        Converts self to a dictionary.
        """
        def factory(fields):
            return {field.metadata.get("json", value.name): value for field, value in fields if value is not MISSING}

        return asdict(
            self,
            dict_factory=factory,
        )

    def as_json(self, include_null=False) -> str:
        """Converts this to json.
        """
        return json.dumps(self.to_dict(include_null))

    @classmethod
    def from_dict(cls: Type[T], json: dict) -> T:
        """Constructs `this` from given json. Assumes camel case convention is used and converts to camel case.

        Args:
            json (dict): Json dictionary

        Raises:
            ValueError: When `this` isn't a dataclass

        Returns:
            T: New instance
        """
        if not is_dataclass(cls):
            raise ValueError(f"{cls.__name__} must be a dataclass")

        field_names = {field.metadata.get("json", field.name) for field in fields(cls)}
        kwargs = {
            camel_to_snake(key): value
            for key, value in json.items()
            if camel_to_snake(key) in field_names
        }
        return cls(**kwargs)

@dataclass
class Request(Base):
    pass

@dataclass
class Paged(Base):
    total: int = field(default=0, metadata={"json": "total"})
    offset: int = field(default=0, metadata={"json": "offset"})
    limit: int = field(default=0, metadata={"json": "limit"})


@dataclass
class Error(Exception):
    code: Optional[int] = field(default=None, metadata={"json": "code"})
    error_code: Optional[int] = field(
        default=None, metadata={"json": "err_code"}
    )
    description: Optional[str] = field(
        default=None, metadata={"json": "description"}
    )


@dataclass
class Response(Base):
    type: str
    error: Optional[Error] = field(default=None)

class Client:
    """
    Provides methods for sending requests and processing responses via JetStream.
    """

    def __init__(
        self,
        inner: Any,
        timeout: float = 1.0,
        prefix: str = DEFAULT_PREFIX
    ) -> None:
        self.inner = inner
        self.timeout = timeout
        self.prefix = None

    async def request(
        self,
        subject: str,
        payload: bytes,
        timeout: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> nats.Msg:
        if timeout is None:
            timeout = self.timeout

        return self.inner.request(subject, payload, timeout=timeout)

    # TODO return `jetstream.Msg`
    async def request_msg(
        self,
        subject: str,
        payload: bytes,
        timeout: Optional[float] = None,
    ) -> Msg:
        if timeout is None:
            timeout = self.timeout

        return self.inner.request(subject, payload, timeout=timeout)

    async def request_json(
        self, subject: str, data: Request, response_type: Type[T],
        timeout: float | None,
        return_exceptions: bool = False,
    ) -> T:
        if self.prefix is not None:
            subject = f"{self.prefix}.{subject}"

        if timeout is None:
            timeout = self.timeout

        request_payload = data.as_json()
        response = await self.inner.request(
            subject, request_payload, timeout=timeout
        )

        return response_type.from_json(response.data)
