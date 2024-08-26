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

from typing import Any, Type, TypeVar

from nats.aio.client import Client as NATS
from typing import Optional

from .api import *
from .stream import (Stream, StreamConfig, StreamInfo, StreamInfoLister, StreamManager, StreamNameAlreadyInUseError, StreamNameLister, StreamNotFoundError, StreamSourceMultipleFilterSubjectsNotSupported, StreamSourceNotSupportedError, StreamSubjectTransformNotSupportedError, _validate_stream_name)
from .consumer import *

class Context(
        # Publisher,
        StreamManager,
        # StreamConsumerManager,
):
    """
   	Provides a context for interacting with JetStream.
	The capabilities of JetStream include:

	- Publishing messages to a stream using `Publisher`.
	- Managing streams using the `StreamManager` protocol.
	- Managing consumers using the `StreamConsumerManager` protocol.
	"""

    def __init__(self, nats: NATS, timeout: float = 2.0):
        self._client = Client(
            nats,
            timeout=timeout,
        )

    async def create_stream(
            self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream:
        """
        Creates a new stream with given config.
        """

        stream_create_subject = f"STREAM.CREATE.{config.name}"
        stream_create_request = config.to_dict()
        try:
            stream_create_response = await self._client.request_json(
                stream_create_subject,
                stream_create_request,
                timeout=timeout
            )
        except JetStreamError as jetstream_error:
            if jetstream_error.code == STREAM_NAME_IN_USE:
                raise StreamNameAlreadyInUseError() from jetstream_error

            raise jetstream_error

        info = StreamInfo.from_dict(stream_create_response)

        # Check if subject transforms are supported
        if config.subject_transform and not info.config.subject_transform:
            raise StreamSubjectTransformNotSupportedError()

        # Check if sources and subject transforms are supported
        if config.sources:
            if not info.config.sources:
                raise StreamSourceNotSupportedError()

            for i in range(len(config.sources)):
                source = config.sources[i]
                response_source = config.sources[i]

                if source.subject_transforms and not response_source.subject_transforms:
                    raise StreamSourceMultipleFilterSubjectsNotSupported()

        return Stream(
            client=self._client,
            name=info.config.name,
            info=info,
        )

    async def update_stream(
        self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream:
        """
        Updates an existing stream with the given config.
        """

        stream_create_subject = f"STREAM.UPDATE.{config.name}"
        stream_create_request = config.to_dict()
        try:
            stream_create_response = await self._client.request_json(
                stream_create_subject,
                stream_create_request,
                timeout=timeout
            )
        except JetStreamError as jetstream_error:
            if jetstream_error.code == STREAM_NAME_IN_USE:
                raise StreamNameAlreadyInUseError() from jetstream_error

            if jetstream_error.code == STREAM_NOT_FOUND:
                raise StreamNotFoundError() from jetstream_error

            raise jetstream_error

        info = StreamInfo.from_dict(stream_create_response)

        # Check if subject transforms are supported
        if config.subject_transform and not info.config.subject_transform:
            raise StreamSubjectTransformNotSupportedError()

        # Check if sources and subject transforms are supported
        if config.sources:
            if not info.config.sources:
                raise StreamSourceNotSupportedError()

            for i in range(len(config.sources)):
                source = config.sources[i]
                response_source = config.sources[i]

                if source.subject_transforms and not response_source.subject_transforms:
                    raise StreamSourceMultipleFilterSubjectsNotSupported()

        return Stream(
            client=self._client,
            name=info.config.name,
            info=info,
        )

    async def create_or_update_stream(
        self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream:
        """Creates a stream with given config or updates it if it already exists."""
        try:
            return await self.update_stream(config, timeout=timeout)
        except StreamNotFoundError:
            return await self.create_stream(config, timeout=timeout)

    async def stream(
        self, name: str, timeout: Optional[float] = None
    ) -> Stream:
        """Fetches `StreamInfo` and returns a `Stream` instance for a given stream name."""
        _validate_stream_name(name)

        stream_info_subject = f"STREAM.INFO.{name}"
        stream_info_request = {}
        try:
            stream_info_response = await self._client.request_json(
                stream_info_subject,
                stream_info_request,
                timeout=timeout
            )
        except JetStreamError as jetstream_error:
            if jetstream_error.code == STREAM_NOT_FOUND:
                raise StreamNotFoundError() from jetstream_error

            raise jetstream_error

        info = StreamInfo.from_dict(stream_info_response)

        return Stream(
            client=self._client,
            name=info.config.name,
            info=info,
        )

    async def stream_name_by_subject(
        self, subject: str, timeout: Optional[float] = None
    ) -> str:
        """Returns a stream name listening on a given subject."""
        raise NotImplementedError

    async def delete_stream(
        self, name: str, timeout: Optional[float] = None
    ) -> None:
        """Removes a stream with given name."""
        _validate_stream_name(name)

        stream_delete_subject = f"STREAM.DELETE.{name}"
        stream_delete_request = {}
        try:
            stream_delete_response = await self._client.request_json(
                stream_delete_subject,
                stream_delete_request,
                timeout=timeout
            )
        except JetStreamError as response_error:
            if response_error.code == STREAM_NOT_FOUND:
                raise StreamNotFoundError() from response_error

            raise response_error

    def list_streams(self,
                     timeout: Optional[float] = None
                     ) -> StreamInfoLister:
        raise NotImplementedError


    def stream_names(self,
                     timeout: Optional[float] = None) -> StreamNameLister:
        raise NotImplementedError
