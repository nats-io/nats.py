import nats.aio.client
import nats.aio.msg

import json
from typing import Optional, Any, Dict

DEFAULT_PREFIX = "$JS.API"

# Error codes
JETSTREAM_NOT_ENABLED_FOR_ACCOUNT = 10039
JETSTREAM_NOT_ENABLED = 10076
STREAM_NOT_FOUND = 10059
STREAM_NAME_IN_USE = 10058
CONSUMER_CREATE = 10012
CONSUMER_NOT_FOUND = 10014
CONSUMER_NAME_EXISTS = 10013
CONSUMER_ALREADY_EXISTS = 10105
CONSUMER_EXISTS = 10148
DUPLICATE_FILTER_SUBJECTS = 10136
OVERLAPPING_FILTER_SUBJECTS = 10138
CONSUMER_EMPTY_FILTER = 10139
CONSUMER_DOES_NOT_EXIST = 10149
MESSAGE_NOT_FOUND = 10037
BAD_REQUEST = 10003
STREAM_WRONG_LAST_SEQUENCE = 10071

# TODO: What should we call this error type?
class JetStreamError(Exception):
    code:str
    description: str

    def __init__(self, code: str, description: str) -> None:
        self.code = code
        self.description = description

    def __str__(self) -> str:
        return (
            f"nats: {type(self).__name__}: code={self.code} "
            f"description='{self.description}'"
        )

class Client:
    """
    Provides methods for sending requests and processing responses via JetStream.
    """

    def __init__(
        self,
        inner: nats.aio.client.Client,
        timeout: float = 2.0,
        prefix: str = DEFAULT_PREFIX
    ) -> None:
        self.inner = inner
        self.timeout = timeout
        self.prefix = prefix

    async def request(
        self,
        subject: str,
        payload: bytes,
        timeout: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> nats.aio.msg.Msg:
        if timeout is None:
            timeout = self.timeout

        return await self.inner.request(subject, payload, timeout=timeout)

    # TODO return `jetstream.Msg`
    async def request_msg(
        self,
        subject: str,
        payload: bytes,
        timeout: Optional[float] = None,
    ) -> nats.aio.msg.Msg:
        return await self.inner.request(subject, payload, timeout=timeout or self.timeout)

    async def request_json(
        self, subject: str, data: Any,
        timeout: float | None,
    ) -> Dict[str, Any]:
        request_subject = f"{self.prefix}.{subject}"
        request_data = json.dumps(data).encode("utf-8")
        response = await self.inner.request(
            request_subject, request_data, timeout or self.timeout
        )

        response_data = json.loads(response.data.decode("utf-8"))
        response_error = response_data.get("error")
        if response_error:
            raise JetStreamError(
                code=response_error["err_code"],
                description=response_error["description"],
            )

        return response_data
