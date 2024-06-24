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

from asyncio import Future
from dataclasses import dataclass, field
from typing import Dict, Optional, cast

from nats.errors import *
from nats.jetstream.api import *
from nats.jetstream.errors import *
from nats.jetstream.message import *

DEFAULT_RETRY_ATTEMPTS = 2

@dataclass
class PubAck:
    """
    PubAck is an ack received after successfully publishing a message.
    """
    stream: str = field(metadata={"json": "stream"})
    """
    The stream name the message was published to.
    """
    sequence: int = field(metadata={"json": "seq"})
    """
    The sequence number of the message.
    """
    duplicate: bool = field(metadata={"json": "duplicate"})
    """
    Indicates whether the message was a duplicate.
    """
    domain: Optional[str] = field(metadata={"json": "domain"})
    """
    The domain the message was published to.
    """

class Publisher:
    def __init__(self, client: Client):
        self.client = client

    async def publish(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: Optional[float] = None,
        headers: Optional[Dict] = None,
        id: Optional[str] = None,
        expected_last_msg_id: Optional[str] = None,
        expected_stream: Optional[str] = None,
        expected_last_sequence: Optional[int] = None,
        expected_last_subject_sequence: Optional[int] = None,
        retry_attempts: int = 2,
        retry_wait: float = 0.25,
    ) -> PubAck:
        """
        Performs a publish to a stream and waits for ack from server.
        """

        extra_headers = {}
        if expected_last_msg_id is not None:
            extra_headers[Header.EXPECTED_LAST_MSG_ID] = str(expected_last_msg_id)

        if expected_stream is not None:
            extra_headers[Header.EXPECTED_STREAM] = str(expected_stream)

        if expected_last_sequence is not None:
            extra_headers[Header.EXPECTED_LAST_SEQ] = str(expected_last_sequence)

        if expected_last_subject_sequence is not None:
            extra_headers[Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(expected_last_subject_sequence)

        if len(extra_headers) > 0:
            if headers is not None:
                extra_headers.update(headers)

            headers = extra_headers

        for attempt in range(0, retry_attempts):
            try:
                msg = await self.client.request(
                    subject,
                    payload,
                    timeout=timeout,
                    headers=headers,
                )

                pub_ack_response = PubAckResponse.from_json(msg.data)
                if pub_ack_response.error is not None:
                    raise Error(**pub_ack_response.error)

                if pub_ack_response.stream == None:
                    raise InvalidAckError()

                return cast(PubAck, pub_ack_response)
            except NoRespondersError:
                if attempt < retry_attempts - 1:
                    await asyncio.sleep(retry_wait)

        raise NoStreamResponseError

class PubAckResponse(Response, PubAck):
    pass
