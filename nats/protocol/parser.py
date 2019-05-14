# Copyright 2016-2018 The NATS Authors
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

import re
import asyncio
import json

MSG_RE = re.compile(
    b'\AMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n'
)
OK_RE = re.compile(b'\A\+OK\s*\r\n')
ERR_RE = re.compile(b'\A-ERR\s+(\'.+\')?\r\n')
PING_RE = re.compile(b'\APING\s*\r\n')
PONG_RE = re.compile(b'\APONG\s*\r\n')
INFO_RE = re.compile(b'\AINFO\s+([^\r\n]+)\r\n')

INFO_OP = b'INFO'
CONNECT_OP = b'CONNECT'
PUB_OP = b'PUB'
MSG_OP = b'MSG'
SUB_OP = b'SUB'
UNSUB_OP = b'UNSUB'
PING_OP = b'PING'
PONG_OP = b'PONG'
OK_OP = b'+OK'
ERR_OP = b'-ERR'
MSG_END = b'\n'
_CRLF_ = b'\r\n'
_SPC_ = b' '

OK = OK_OP + _CRLF_
PING = PING_OP + _CRLF_
PONG = PONG_OP + _CRLF_
CRLF_SIZE = len(_CRLF_)
OK_SIZE = len(OK)
PING_SIZE = len(PING)
PONG_SIZE = len(PONG)
MSG_OP_SIZE = len(MSG_OP)
ERR_OP_SIZE = len(ERR_OP)

# States
AWAITING_CONTROL_LINE = 1
AWAITING_MSG_PAYLOAD = 2
MAX_CONTROL_LINE_SIZE = 1024


class Parser(object):
    def __init__(self, nc=None):
        self.nc = nc
        self.reset()

    def __repr__(self):
        return "<nats protocol parser state={0}>".format(self.state)

    def reset(self):
        self.buf = bytearray()
        self.state = AWAITING_CONTROL_LINE
        self.needed = 0
        self.msg_arg = {}

    @asyncio.coroutine
    def parse(self, data=b''):
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
                        subject, sid, _, reply, needed_bytes = msg.groups()
                        self.msg_arg["subject"] = subject
                        self.msg_arg["sid"] = int(sid)
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

                ok = OK_RE.match(self.buf)
                if ok:
                    # Do nothing and just skip.
                    del self.buf[:ok.end()]
                    continue

                err = ERR_RE.match(self.buf)
                if err:
                    err_msg = err.groups()
                    yield from self.nc._process_err(err_msg)
                    del self.buf[:err.end()]
                    continue

                ping = PING_RE.match(self.buf)
                if ping:
                    del self.buf[:ping.end()]
                    yield from self.nc._process_ping()
                    continue

                pong = PONG_RE.match(self.buf)
                if pong:
                    del self.buf[:pong.end()]
                    yield from self.nc._process_pong()
                    continue

                info = INFO_RE.match(self.buf)
                if info:
                    info_line = info.groups()[0]
                    srv_info = json.loads(info_line.decode())
                    self.nc._process_info(srv_info)
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
                    subject = self.msg_arg["subject"]
                    sid = self.msg_arg["sid"]
                    reply = self.msg_arg["reply"]

                    # Consume msg payload from buffer and set next parser state.
                    payload = bytes(self.buf[:self.needed])
                    del self.buf[:self.needed + CRLF_SIZE]
                    self.state = AWAITING_CONTROL_LINE
                    yield from self.nc._process_msg(
                        sid, subject, reply, payload
                    )
                else:
                    # Wait until we have enough bytes in buffer.
                    break


class ErrProtocol(Exception):
    def __str__(self):
        return "nats: Protocol Error"
