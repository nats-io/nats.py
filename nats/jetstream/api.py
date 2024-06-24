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

import json

from dataclasses import dataclass, fields, is_dataclass, MISSING
from typing import Any, Dict, Type, TypeVar, get_origin, get_args

from nats.aio.client import Client
from nats.jetstream.errors import Error

T = TypeVar("T")

STREAM_CREATE = "STREAM.CREATE.%s"

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

def from_dict(data, cls: Type[T]) -> T:
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
        elif field_origin is list and len(field_args) == 1 and is_dataclass(field_args[0]):
            value = [from_dict(item, field_args[0]) for item in value]
        elif field_origin is dict and len(field_args) == 2 and is_dataclass(field_args[1]):
            value = {k: from_dict(v, field_args[1]) for k, v in value.items()}

        kwargs[field.name] = value

    return cls(**kwargs)

def parse_json_response(response: str | bytes | bytearray, cls: type[T]) -> T:
    json_response = json.loads(response)
    if 'error' in json_response:
        raise from_dict(json_response['error'], Error)

    return from_dict(json_response, cls)

async def request_json(client: Client, subject: str, item: Any, cls: Type[T], timeout: float = 5.0) -> T:
    json_data = as_dict(item)
    json_payload = json.dumps(json_data).encode()
    response = await client.request(subject, json_payload, timeout=timeout)
    return parse_json_response(response.data, cls)

def subject(prefix: str | None, template: str, *args) -> str:
    value = template.format(args)

    if prefix is None:
        return value

    return f"{prefix}.{value}"
