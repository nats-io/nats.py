# Copyright 2016-2021 The NATS Authors
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
"""
NATS network protocol parser.
"""
import json
from email.parser import BytesParser
from typing import TYPE_CHECKING, Any, Dict, Optional

from .constants import (
    _CRLF_, AWAITING_CONTROL_LINE, AWAITING_MSG_PAYLOAD, CRLF_SIZE, CTRL_LEN,
    DESC_HDR, ERR_RE, HMSG_RE, INFO_RE, MAX_CONTROL_LINE_SIZE, MSG_RE,
    NATS_HDR_LINE, OK_RE, PING_RE, PONG_RE, STATUS_HDR, STATUS_MSG_LEN
)

if TYPE_CHECKING:
    from nats.aio.client import Client


class Parser:
    def __init__(self, nc: Optional["Client"] = None) -> None:
        self.nc = nc
        self.reset()

    def __repr__(self) -> str:
        return f"<nats protocol parser state={self.state}>"

    def reset(self) -> None:
        self.buf = bytearray()
        self.state = AWAITING_CONTROL_LINE
        self.needed = 0
        self.header_needed = 0
        self.msg_arg: Dict[str, Any] = {}

    async def parse(self, data: bytes = b'') -> None:
        """
        Parses the wire protocol from NATS for the client
        and dispatches the subscription callbacks.
        """
        self.buf.extend(data)
        while self.buf:
            if self.state == AWAITING_CONTROL_LINE:
                msg = MSG_RE.match(self.buf)
                if msg:
                    try:
                        subject, _sid, _, reply, needed_bytes = msg.groups()
                        self.msg_arg["subject"] = subject
                        self.msg_arg["sid"] = int(_sid)
                        if reply:
                            self.msg_arg["reply"] = reply
                        else:
                            self.msg_arg["reply"] = b''
                        self.needed = int(needed_bytes)
                        del self.buf[:msg.end()]
                        self.state = AWAITING_MSG_PAYLOAD
                        continue
                    except:
                        raise ErrProtocol("nats: malformed MSG")

                msg = HMSG_RE.match(self.buf)
                if msg:
                    try:
                        subject, _sid, _, reply, header_size, needed_bytes = msg.groups(
                        )
                        self.msg_arg["subject"] = subject
                        self.msg_arg["sid"] = int(_sid)
                        if reply:
                            self.msg_arg["reply"] = reply
                        else:
                            self.msg_arg["reply"] = b''
                        self.needed = int(needed_bytes)
                        self.header_needed = int(header_size)
                        del self.buf[:msg.end()]
                        self.state = AWAITING_MSG_PAYLOAD
                        continue
                    except:
                        raise ErrProtocol("nats: malformed MSG")

                ok = OK_RE.match(self.buf)
                if ok:
                    # Do nothing and just skip.
                    del self.buf[:ok.end()]
                    continue

                err = ERR_RE.match(self.buf)
                if err:
                    err_msg = err.groups()
                    await self.nc._process_err(  # type: ignore[union-attr]
                        err_msg
                    )
                    del self.buf[:err.end()]
                    continue

                ping = PING_RE.match(self.buf)
                if ping:
                    del self.buf[:ping.end()]
                    await self.nc._process_ping()  # type: ignore[union-attr]
                    continue

                pong = PONG_RE.match(self.buf)
                if pong:
                    del self.buf[:pong.end()]
                    await self.nc._process_pong()  # type: ignore[union-attr]
                    continue

                info = INFO_RE.match(self.buf)
                if info:
                    info_line = info.groups()[0]
                    srv_info = json.loads(info_line.decode())
                    self.nc._process_info(srv_info)  # type: ignore[union-attr]
                    del self.buf[:info.end()]
                    continue

                if len(self.buf
                       ) < MAX_CONTROL_LINE_SIZE and _CRLF_ in self.buf:
                    # FIXME: By default server uses a max protocol
                    # line of 1024 bytes but it can be tuned in latest
                    # releases, in that case we won't reach here but
                    # client ping/pong interval would disconnect
                    # eventually.
                    raise ErrProtocol("nats: unknown protocol")
                else:
                    # If nothing matched at this point, then it must
                    # be a split buffer and need to gather more bytes.
                    break

            elif self.state == AWAITING_MSG_PAYLOAD:
                if len(self.buf) >= self.needed + CRLF_SIZE:
                    hdr = None
                    subject = self.msg_arg["subject"]
                    sid = self.msg_arg["sid"]
                    reply = self.msg_arg["reply"]

                    # Consume msg payload from buffer and set next parser state.
                    if self.header_needed > 0:
                        hbuf = bytes(self.buf[:self.header_needed])
                        payload = bytes(
                            self.buf[self.header_needed:self.needed]
                        )
                        hdr = hbuf
                        del self.buf[:self.needed + CRLF_SIZE]
                        self.header_needed = 0
                    else:
                        payload = bytes(self.buf[:self.needed])
                        del self.buf[:self.needed + CRLF_SIZE]

                    self.state = AWAITING_CONTROL_LINE
                    await self.nc._process_msg(  # type: ignore[union-attr]
                        sid,
                        subject,
                        reply,
                        payload,
                        hdr
                    )
                else:
                    # Wait until we have enough bytes in buffer.
                    break


class ErrProtocol(Exception):
    def __str__(self) -> str:
        return "nats: Protocol Error"


class HeaderParser:
    def __init__(self, bytes_parser: Optional[BytesParser] = None) -> None:
        self._bytes_parser = bytes_parser or BytesParser()

    def parse(self, headers: Optional[bytes]) -> Dict[str, str]:
        hdrs: Dict[str, str] = {}
        if headers is None:
            return hdrs
        raw_headers = headers[len(NATS_HDR_LINE):]
        parsed_hdrs = self._bytes_parser.parsebytes(raw_headers)
        # Check if it is an inline status message like:
        #
        # NATS/1.0 404 No Messages
        #
        if len(parsed_hdrs.items()) == 0:
            l = headers[len(NATS_HDR_LINE) - 1:]
            status = l[:STATUS_MSG_LEN]
            desc = l[STATUS_MSG_LEN + 1:len(l) - CTRL_LEN - CTRL_LEN]
            hdrs[STATUS_HDR] = status.decode()
            hdrs[DESC_HDR] = desc.decode()
        else:
            for k, v in parsed_hdrs.items():
                hdrs[k] = v

        return hdrs
