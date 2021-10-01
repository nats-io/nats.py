import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Dict, Optional, Type, TypeVar

from nats.aio.defaults import JS_API_PREFIX as DEFAULT_JS_API_PREFIX
from nats.aio.errors import JetStreamAPIError
from nats.aio.js.models.account_info import AccountInfo
from nats.aio.messages import Msg

from .api.consumer import ConsumerAPI
from .api.kv import KeyValueAPI
from .api.stream import StreamAPI
from .api.utils import check_js_msg

if TYPE_CHECKING:
    from nats.aio.client import Client as NC  # pragma: no cover

RequestT = TypeVar("RequestT", bound=Any)
ResponseT = TypeVar("ResponseT", bound=Any)


class JetStream:
    def __init__(
        self,
        client: "NC",
        domain: Optional[str] = None,
    ) -> None:
        self._nc = client
        self._prefix = f"$JS.{domain}.API" if domain else DEFAULT_JS_API_PREFIX
        self.domain = domain
        self.consumer = ConsumerAPI(self)
        self.stream = StreamAPI(self)
        self.kv = KeyValueAPI(self)

    async def account_info(self, timeout: float = 1) -> AccountInfo:
        """Account information"""
        return await self._request(
            "INFO", timeout=timeout, response_dc=AccountInfo
        )

    async def _request(
        self,
        subject: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 1,
        headers: Optional[Dict[str, str]] = None,
        request_dc: Optional[Type[RequestT]] = None,
        response_dc: Optional[Type[ResponseT]] = None,
    ) -> ResponseT:
        """Request a message against JetStream API and validate response."""
        # Validate request payload
        if request_dc:
            params = {} if params is None else params
            params = asdict(request_dc(**params))
        # Encode payload
        payload = json.dumps(params
                             ).encode("utf-8") if params is not None else b""
        # Send request
        msg = await self._nc.request(
            f"{self._prefix}.{subject}",
            payload=payload,
            timeout=timeout,
            headers=headers,
        )
        # Check for errors
        check_js_msg(msg)
        # Do not parse when response param is Msg
        if response_dc is None:
            return msg  # type: ignore[return-value]
        # Else parse JSON
        data = json.loads(msg.data)
        # Raise errors when needed
        if "error" in data:
            raise JetStreamAPIError(
                code=data["error"].get("code"),
                description=data["error"].get("description")
            )
        # Parse expected structure
        result: ResponseT = response_dc(**data)
        return result
