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
from dataclasses import MISSING, dataclass, field, fields, is_dataclass
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

from .api import DEFAULT_PREFIX
from .message import Msg


def as_dict(instance: Any) -> Dict[str, Any]:
    if not is_dataclass(instance):
        return instance

    result = {}
    for field in fields(instance):
        name = field.metadata.get('json', field.name)
        value = getattr(instance, field.name)
        if is_dataclass(value):
            result[name] = as_dict(value)
        elif isinstance(value, list):
            result[name] = [as_dict(item) for item in value]
        elif isinstance(value, dict):
            result[name] = {k: as_dict(v) for k, v in value.items()}
        else:
            result[name] = value
    return result


def from_dict(data, cls: type) -> Any:
    if not is_dataclass(cls):
        return data

    kwargs = {}
    for field in fields(cls):
        json_key = field.metadata.get('json', field.name)
        value = data.get(json_key, MISSING)

        if value is MISSING:
            if field.default is not MISSING:
                value = field.default
            elif field.default_factory is not MISSING:
                value = field.default_factory()
            else:
                raise ValueError(f"Missing value for field {field.name}")

        field_type = field.type
        field_origin = get_origin(field_type)
        field_args = get_args(field_type)

        if is_dataclass(field_type):
            value = from_dict(value, field_type)
        elif field_origin is list and len(field_args) == 1 and is_dataclass(
                field_args[0]):
            value = [from_dict(item, field_args[0]) for item in value]
        elif field_origin is dict and len(field_args) == 2 and is_dataclass(
                field_args[1]):
            value = {k: from_dict(v, field_args[1]) for k, v in value.items()}

        kwargs[field.name] = value

    return cls(**kwargs)


T = TypeVar("T", bound="Response")


@dataclass
class Request:
    def as_dict(self) -> Dict[str, Any]:
        return as_dict(self)

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


@dataclass
class Paged:
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
class Response:
    type: str
    error: Optional[Error] = field(default=None)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Create an instance of the class from a dictionary.
        """
        return cls(**data)

    @classmethod
    def from_json(cls: Type[T], data: str) -> T:
        """
        Create an instance of the class from JSON
        """
        return cls.from_dict(json.loads(data))

    def handle_error(self):
        if self.error:
            raise self.error

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
        timeout: float | None
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
