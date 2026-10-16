from __future__ import annotations

import json
import logging
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeGuard,
    Unpack,
    cast,
    overload,
)

from ..errors import (
    JetStreamError,
)
from .types import (
    AccountInfoResponse,
    ConsumerCreateRequest,
    ConsumerCreateResponse,
    ConsumerDeleteResponse,
    ConsumerInfoResponse,
    ConsumerListRequest,
    ConsumerListResponse,
    ConsumerNamesRequest,
    ConsumerNamesResponse,
    ConsumerPauseRequest,
    ConsumerPauseResponse,
    ConsumerResetRequest,
    ConsumerResetResponse,
    ErrorResponse,
    StreamCreateRequest,
    StreamCreateResponse,
    StreamDeleteResponse,
    StreamInfoRequest,
    StreamInfoResponse,
    StreamListRequest,
    StreamListResponse,
    StreamMsgDeleteRequest,
    StreamMsgDeleteResponse,
    StreamMsgGetRequest,
    StreamMsgGetResponse,
    StreamNamesRequest,
    StreamNamesResponse,
    StreamPurgeRequest,
    StreamPurgeResponse,
    StreamUpdateRequest,
    StreamUpdateResponse,
)
from .types import (
    Error as ApiError,
)

if TYPE_CHECKING:
    from nats.client import Client as NatsClient

logger = logging.getLogger("nats.jetstream.api")


def _error_from_response(error: ApiError, *, strict: bool = False) -> JetStreamError:
    """Parse an error response and return a JetStreamError instance."""
    description = error.pop("description", "Unknown error")
    status_code = error.pop("code", None)  # API status code (400, 404, 503)
    err_code = error.pop("err_code", None)  # JetStream error code (10003, 10014, 10059)

    # Check for unconsumed fields
    if strict and error:
        raise ValueError(f"Error.from_response() has unconsumed fields: {list(error.keys())}")

    return JetStreamError(
        message=description,
        code=status_code,
        error_code=err_code,
        description=description,
    )


def is_error_response(data: Any) -> TypeGuard[ErrorResponse]:
    return isinstance(data, dict) and "error" in data


def check_response[ResponseT](data: Any, expected_type: type[ResponseT]) -> tuple[bool, set[str], set[str]]:
    if not isinstance(data, dict):
        return False, set(), set()

    # Get expected keys from TypedDict annotations
    expected_keys = set(expected_type.__annotations__.keys())
    actual_keys = set(data.keys())

    # Find missing required keys and unknown keys
    missing_keys = expected_keys - actual_keys
    unknown_keys = actual_keys - expected_keys

    # Filter out optional keys from missing_keys
    # TypedDict stores required/optional info in __required_keys__ (Python 3.9+)
    if hasattr(expected_type, "__required_keys__"):
        required_keys = expected_type.__required_keys__
        missing_keys = missing_keys & required_keys

    is_valid = len(missing_keys) == 0 and len(unknown_keys) == 0

    return is_valid, unknown_keys, missing_keys


class Client:
    def __init__(
        self,
        client: NatsClient,
        prefix: str = "$JS.API",
        validate_response: bool = False,
        raise_on_missing_keys: bool = False,
        raise_on_unknown_keys: bool = False,
    ) -> None:
        self._client = client
        self._prefix = prefix
        self._validate_response = validate_response
        self._raise_on_missing_keys = raise_on_missing_keys
        self._raise_on_unknown_keys = raise_on_unknown_keys

    async def account_info(self) -> AccountInfoResponse:
        return await self.request_json(
            f"{self._prefix}.INFO",
            response_type=AccountInfoResponse,
        )

    async def consumer_create(
        self, stream_name: str, consumer_name: str, /, **request: Unpack[ConsumerCreateRequest]
    ) -> ConsumerCreateResponse:
        return await self.request_json(
            f"{self._prefix}.CONSUMER.CREATE.{stream_name}.{consumer_name}",
            request,
            response_type=ConsumerCreateResponse,
        )

    async def consumer_delete(self, stream_name: str, consumer_name: str, /) -> ConsumerDeleteResponse:
        return await self.request_json(
            f"{self._prefix}.CONSUMER.DELETE.{stream_name}.{consumer_name}",
            response_type=ConsumerDeleteResponse,
        )

    async def consumer_info(self, stream_name: str, consumer_name: str, /) -> ConsumerInfoResponse:
        return await self.request_json(
            f"{self._prefix}.CONSUMER.INFO.{stream_name}.{consumer_name}",
            response_type=ConsumerInfoResponse,
        )

    async def consumer_list(self, stream_name: str, /, **request: Unpack[ConsumerListRequest]) -> ConsumerListResponse:
        """Get information about all consumers in a stream."""
        return await self.request_json(
            f"{self._prefix}.CONSUMER.LIST.{stream_name}",
            request if request else None,
            response_type=ConsumerListResponse,
        )

    async def consumer_names(
        self, stream_name: str, /, **request: Unpack[ConsumerNamesRequest]
    ) -> ConsumerNamesResponse:
        """Get a list of all consumer names in a stream."""
        return await self.request_json(
            f"{self._prefix}.CONSUMER.NAMES.{stream_name}",
            request if request else None,
            response_type=ConsumerNamesResponse,
        )

    async def consumer_pause(
        self, stream_name: str, consumer_name: str, /, **request: Unpack[ConsumerPauseRequest]
    ) -> ConsumerPauseResponse:
        """Pause or resume a consumer.

        Args:
            stream_name: The stream name
            consumer_name: The consumer name
            **request: Request body with optional pause_until field

        Returns:
            ConsumerPauseResponse with pause state
        """
        return await self.request_json(
            f"{self._prefix}.CONSUMER.PAUSE.{stream_name}.{consumer_name}",
            request if request else None,
            response_type=ConsumerPauseResponse,
        )

    async def consumer_reset(
        self, stream_name: str, consumer_name: str, /, **request: Unpack[ConsumerResetRequest]
    ) -> ConsumerResetResponse:
        """Reset a consumer's delivery state (ADR-60).

        Args:
            stream_name: The stream name
            consumer_name: The consumer name
            **request: Request body with optional ``seq`` field. Empty payload
                or ``seq=0`` resumes redelivery from one above the consumer's
                ack floor; a non-zero ``seq`` sets the ack floor to one below
                that sequence so the next delivered message has a stream
                sequence ``>= seq``.

        Returns:
            ConsumerResetResponse with refreshed consumer state and the
            stream sequence the consumer was reset to.
        """
        return await self.request_json(
            f"{self._prefix}.CONSUMER.RESET.{stream_name}.{consumer_name}",
            request if request else None,
            response_type=ConsumerResetResponse,
        )

    async def stream_create(self, name: str, /, **request: Unpack[StreamCreateRequest]) -> StreamCreateResponse:
        return await self.request_json(
            f"{self._prefix}.STREAM.CREATE.{name}",
            request,
            response_type=StreamCreateResponse,
        )

    async def stream_delete(self, name: str, /) -> StreamDeleteResponse:
        return await self.request_json(
            f"{self._prefix}.STREAM.DELETE.{name}",
            response_type=StreamDeleteResponse,
        )

    async def stream_info(self, name: str, /, **request: Unpack[StreamInfoRequest]) -> StreamInfoResponse:
        return await self.request_json(
            f"{self._prefix}.STREAM.INFO.{name}",
            request if request else None,
            response_type=StreamInfoResponse,
        )

    async def stream_list(self, **request: Unpack[StreamListRequest]) -> StreamListResponse:
        """Get information about all streams.

        Args:
            **request: Request body with optional offset and subject fields

        Returns:
            Response containing stream information and pagination info
        """
        return await self.request_json(
            f"{self._prefix}.STREAM.LIST",
            request if request else None,
            response_type=StreamListResponse,
        )

    async def stream_msg_delete(
        self, name: str, /, **request: Unpack[StreamMsgDeleteRequest]
    ) -> StreamMsgDeleteResponse:
        return await self.request_json(
            f"{self._prefix}.STREAM.MSG.DELETE.{name}",
            request if request else None,
            response_type=StreamMsgDeleteResponse,
        )

    async def stream_msg_get(self, name: str, /, **request: Unpack[StreamMsgGetRequest]) -> StreamMsgGetResponse:
        return await self.request_json(
            f"{self._prefix}.STREAM.MSG.GET.{name}",
            request if request else None,
            response_type=StreamMsgGetResponse,
        )

    async def stream_names(self, **request: Unpack[StreamNamesRequest]) -> StreamNamesResponse:
        """Get a list of all stream names.

        Args:
            **request: Request body with optional offset and subject fields

        Returns:
            Response containing stream names and pagination info
        """
        return await self.request_json(
            f"{self._prefix}.STREAM.NAMES",
            request if request else None,
            response_type=StreamNamesResponse,
        )

    async def stream_purge(self, name: str, /, **request: Unpack[StreamPurgeRequest]) -> StreamPurgeResponse:
        return await self.request_json(
            f"{self._prefix}.STREAM.PURGE.{name}",
            request if request else None,
            response_type=StreamPurgeResponse,
        )

    async def stream_update(self, name: str, /, **request: Unpack[StreamUpdateRequest]) -> StreamUpdateResponse:
        return await self.request_json(
            f"{self._prefix}.STREAM.UPDATE.{name}",
            request,
            response_type=StreamUpdateResponse,
        )

    @overload
    async def request_json[ResponseT](
        self,
        subject: str,
        payload: Any | None = None,
        *,
        response_type: type[ResponseT],
        raise_on_error: Literal[True] = True,
    ) -> ResponseT: ...

    @overload
    async def request_json[ResponseT](
        self,
        subject: str,
        payload: Any | None = None,
        *,
        response_type: type[ResponseT],
        raise_on_error: Literal[False],
    ) -> ResponseT | ErrorResponse: ...

    async def request_json[ResponseT](
        self,
        subject: str,
        payload: Any | None = None,
        *,
        response_type: type[ResponseT],
        raise_on_error: bool = True,
        timeout: float = 5.0,
    ) -> ResponseT | ErrorResponse:
        request_id = str(uuid.uuid4())[:8]
        encoded_payload = json.dumps(payload).encode() if payload is not None else b""
        logger.debug("[%s] request: %s %r", request_id, subject, encoded_payload)
        response = await self._client.request(subject, encoded_payload, timeout=timeout)

        data = json.loads(response.data.decode())
        logger.debug("[%s] response: %r", request_id, data)

        if raise_on_error and is_error_response(data):
            raise _error_from_response(data["error"])

        if self._validate_response:
            is_valid, unknown_keys, missing_keys = check_response(data, response_type)
            if not is_valid:
                if missing_keys:
                    msg = f"Missing required keys in response: {missing_keys}"
                    if self._raise_on_missing_keys:
                        raise ValueError(msg)
                    logger.warning("[%s] %s", request_id, msg)
                if not missing_keys and not unknown_keys:
                    logger.warning("[%s] Expected %s, got %s", request_id, response_type.__name__, type(data).__name__)

            if unknown_keys:
                msg = f"Unknown keys in response: {unknown_keys}"
                if self._raise_on_unknown_keys:
                    raise ValueError(msg)
                logger.warning("[%s] %s", request_id, msg)

        return cast(ResponseT, data)
