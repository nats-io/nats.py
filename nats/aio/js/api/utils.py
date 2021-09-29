from typing import TYPE_CHECKING, Dict, Optional

from nats.aio.errors import JetStreamAPIError
from nats.protocol.constants import DESC_HDR, STATUS_HDR

if TYPE_CHECKING:
    from nats.aio.messages import Msg  # pragma: no cover


def check_js_headers(headers: Optional[Dict[str, str]]) -> None:
    if headers is None:
        return
    if STATUS_HDR in headers:
        code = headers[STATUS_HDR]
        if code[0] == "2":
            return
        desc = headers[DESC_HDR]
        raise JetStreamAPIError(code=code, description=desc)


def check_js_msg(msg: "Msg") -> None:
    if len(msg.data) == 0:
        check_js_headers(msg.headers)
