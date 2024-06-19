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

from asyncio import Future
from dataclasses import dataclass
from nats.aio.client import Client
from nats.errors import NoRespondersError
from nats.jetstream.errors import APIError, NoStreamResponseError
from nats.jetstream.message import Msg
from typing import Optional

import json

@dataclass
class PubAck:
    """
    Represents an acknowledgment received after successfully publishing a message.
    """

    stream: str
    """
    str: The name of the stream to which the message was published.
    """

    sequence: int
    """
    int: The sequence number of the message in the stream.
    """

    duplicate: bool = False
    """
    bool: Indicates whether the message was a duplicate. Defaults to False.
    """

    domain: str = ""
    """
    str: The domain to which the message was published. Defaults to an empty string.
    """

class Publisher:
    def __init__(self, client: Client, max_pending_acks: int = 4000):
        self.client = client

    async def publish(
        self,
        subject: str,
        payload: bytes,
        timeout: float = 1.0,
    ) -> PubAck:
        """
        Performs a publish to a stream and waits for ack from server.
        """
        try:
            msg = await self.client.request(
                subject,
                payload,
                timeout=timeout,
            )

            data = json.loads(msg.data)
            if 'error' in data:
                raise APIError.from_error(data['error'])

            return PubAck(**data)
        except NoRespondersError:
            raise NoStreamResponseError

        raise NotImplementedError

    async def publish_async(self, subject: str, payload: bytes) -> Future[PubAck]:
        """
        Performs a publish to a stream returning a future that can be awaited for the ack from server.
        """
        raise NotImplementedError

    async def publish_async_pending(self) -> int:
        """
        Returns the number of async publishes outstanding for this context.
        An outstanding publish is one that has been sent by the publisher but has not yet received an ack.
        """
        raise NotImplementedError

    async def publish_async_complete(self) -> None:
        """
        Returns a future that will be closed when all outstanding asynchronously published messages are acknowledged by the server.
        """
        raise NotImplementedError
